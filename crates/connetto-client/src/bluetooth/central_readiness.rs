//! A readiness that runs no manager of its own, its permission read from the
//! platform and its power reported by the running central, Apple's (R77
//! decision 23, R76's Apple readiness table).

use std::sync::Arc;

use parking_lot::Mutex;

use super::{BluetoothError, BluetoothState, PowerReport, PromptOutcome, ReadinessBackend};

/// The permission a `NotPermitted` standing names.
const BLUETOOTH_PERMISSION: &str = "Bluetooth";

/// The app's Bluetooth permission, as the platform answers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Permission {
    /// The user has not been asked yet.
    Unasked,
    /// The user or a policy refused it.
    Refused,
    /// The app may use Bluetooth.
    Allowed,
}

/// What the central and the prompts left standing.
#[derive(Default)]
struct Standing {
    /// The radio's power the central last reported, `None` while unknown.
    power: Option<bool>,
    /// A prompt asked for the permission, so the central's start may ask.
    asked: bool,
    /// A prompt asked for the running central's restart.
    restart: bool,
}

/// The readiness over the platform's permission read and the central's
/// power report (R76's Apple readiness table).
pub(crate) struct CentralReadiness {
    permission: Box<dyn Fn() -> Permission + Send + Sync>,
    standing: Arc<Mutex<Standing>>,
}

impl CentralReadiness {
    /// The readiness over `permission`, the platform's read.
    pub(crate) fn new(permission: impl Fn() -> Permission + Send + Sync + 'static) -> Self {
        Self {
            permission: Box::new(permission),
            standing: Arc::new(Mutex::new(Standing::default())),
        }
    }

    /// The report the central's thread writes the radio's power through.
    pub(crate) fn power_report(&self) -> PowerReport {
        let standing = Arc::clone(&self.standing);
        Arc::new(move |power| standing.lock().power = power)
    }
}

impl ReadinessBackend for CentralReadiness {
    fn state(&self) -> BluetoothState {
        match (self.permission)() {
            Permission::Unasked | Permission::Refused => BluetoothState::NotPermitted,
            Permission::Allowed => match self.standing.lock().power {
                Some(true) => BluetoothState::Ready,
                // Off, or unknown until the central's start reports it.
                Some(false) | None => BluetoothState::Off,
            },
        }
    }

    fn missing_permissions(&self) -> Vec<String> {
        match (self.permission)() {
            Permission::Unasked | Permission::Refused => vec![BLUETOOTH_PERMISSION.to_owned()],
            Permission::Allowed => Vec::new(),
        }
    }

    fn prompt(&self) -> Result<(), BluetoothError> {
        let permission = (self.permission)();
        let mut standing = self.standing.lock();
        match permission {
            // The central's start creates the platform's manager, which asks
            // for the permission.
            Permission::Unasked => standing.asked = true,
            // A new manager shows the platform's power alert again.
            Permission::Allowed if standing.power == Some(false) => standing.restart = true,
            // The platform asks a refused app nothing again, and an unknown
            // or powered radio has nothing to show.
            Permission::Refused | Permission::Allowed => {}
        }
        Ok(())
    }

    fn prompt_outcome(&self) -> Option<PromptOutcome> {
        match (self.permission)() {
            Permission::Unasked => None,
            Permission::Refused => Some(PromptOutcome::Declined),
            Permission::Allowed => match self.standing.lock().power {
                Some(true) => Some(PromptOutcome::NotAsked),
                // The platform reports no decline for its power alert, so
                // the caller's bound ends the wait.
                Some(false) | None => None,
            },
        }
    }

    fn central_may_start(&self) -> bool {
        match (self.permission)() {
            Permission::Allowed => true,
            Permission::Unasked => self.standing.lock().asked,
            Permission::Refused => false,
        }
    }

    fn take_central_restart(&self) -> bool {
        let mut standing = self.standing.lock();
        if !core::mem::take(&mut standing.restart) {
            return false;
        }
        // The new central reports the power again.
        standing.power = None;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A readiness over a permission the test steers.
    fn readiness(permission: Permission) -> (CentralReadiness, Arc<Mutex<Permission>>) {
        let steered = Arc::new(Mutex::new(permission));
        let read = Arc::clone(&steered);
        (CentralReadiness::new(move || *read.lock()), steered)
    }

    #[test]
    fn an_unasked_permission_reads_not_permitted_and_starts_the_central_only_after_a_prompt() {
        let (r, _) = readiness(Permission::Unasked);
        assert_eq!(r.state(), BluetoothState::NotPermitted);
        assert_eq!(r.missing_permissions(), vec!["Bluetooth".to_owned()]);
        assert!(!r.central_may_start(), "no permission ask outside a call");
        r.prompt().unwrap();
        assert!(r.central_may_start());
        assert_eq!(r.prompt_outcome(), None);
    }

    #[test]
    fn a_refused_permission_never_starts_the_central_and_declines() {
        let (r, _) = readiness(Permission::Refused);
        r.prompt().unwrap();
        assert_eq!(r.state(), BluetoothState::NotPermitted);
        assert!(!r.central_may_start());
        assert_eq!(r.prompt_outcome(), Some(PromptOutcome::Declined));
        assert!(!r.take_central_restart());
    }

    #[test]
    fn an_allowed_permission_starts_the_central_and_reads_off_until_the_power_is_known() {
        let (r, _) = readiness(Permission::Allowed);
        assert!(r.central_may_start());
        assert_eq!(r.state(), BluetoothState::Off);
        assert_eq!(r.prompt_outcome(), None);
        r.prompt().unwrap();
        assert!(
            !r.take_central_restart(),
            "an unknown power restarts nothing"
        );
    }

    #[test]
    fn the_reported_power_decides_ready_and_off() {
        let (r, _) = readiness(Permission::Allowed);
        let report = r.power_report();
        report(Some(true));
        assert_eq!(r.state(), BluetoothState::Ready);
        assert_eq!(r.prompt_outcome(), Some(PromptOutcome::NotAsked));
        report(Some(false));
        assert_eq!(r.state(), BluetoothState::Off);
        report(None);
        assert_eq!(r.state(), BluetoothState::Off);
    }

    #[test]
    fn a_prompt_with_the_radio_off_asks_one_restart_that_forgets_the_power() {
        let (r, _) = readiness(Permission::Allowed);
        r.power_report()(Some(false));
        r.prompt().unwrap();
        assert!(r.take_central_restart());
        assert!(!r.take_central_restart(), "the ask is taken once");
        assert_eq!(r.state(), BluetoothState::Off);
        r.prompt().unwrap();
        assert!(
            !r.take_central_restart(),
            "the forgotten power asks nothing"
        );
    }

    #[test]
    fn a_prompt_with_the_radio_on_asks_no_restart() {
        let (r, _) = readiness(Permission::Allowed);
        r.power_report()(Some(true));
        r.prompt().unwrap();
        assert!(!r.take_central_restart());
    }

    #[test]
    fn the_permission_is_read_on_every_look() {
        let (r, steered) = readiness(Permission::Unasked);
        r.prompt().unwrap();
        *steered.lock() = Permission::Allowed;
        r.power_report()(Some(true));
        assert_eq!(r.state(), BluetoothState::Ready);
        assert_eq!(r.missing_permissions(), Vec::<String>::new());
        *steered.lock() = Permission::Refused;
        assert_eq!(r.state(), BluetoothState::NotPermitted);
        assert_eq!(r.prompt_outcome(), Some(PromptOutcome::Declined));
    }
}
