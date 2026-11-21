import XCTest

/// One error the tapper names in the test log the proof keeps.
enum TapperError: LocalizedError {
    case action(String)

    var errorDescription: String? {
        switch self {
        case .action(let message): message
        }
    }
}

/// Taps a system alert with a known button, one action per start. The action
/// and its arguments arrive as `TEST_RUNNER_` environment variables the
/// proof writes into the run's xctestrun file, since a shell environment does
/// not cross from the Mac to the device.
@MainActor
final class TapperTests: XCTestCase {
    func testAction() throws {
        let environment = ProcessInfo.processInfo.environment
        let bound = bounded(environment["TEST_RUNNER_TIMEOUT"], default: 90)
        let action = environment["TEST_RUNNER_ACTION"] ?? ""
        switch action {
        case "tap-alert", "tap-join-alert":
            let labels = labels(environment["TEST_RUNNER_LABELS"])
            guard !labels.isEmpty else {
                throw TapperError.action("TEST_RUNNER_LABELS names no button")
            }
            try tapAlert(labels: labels, bound: bound, minimum: action == "tap-join-alert" ? bound : 10)
        default:
            throw TapperError.action(
                "the tapper action is missing or unsupported (\(action))"
            )
        }
    }

    /// The bound in seconds, clamped so a bad value cannot hang the run.
    private func bounded(_ raw: String?, default defaultValue: TimeInterval) -> TimeInterval {
        let value = raw.flatMap(TimeInterval.init) ?? defaultValue
        return min(max(value, 5), 600)
    }

    private func labels(_ raw: String?) -> [String] {
        (raw ?? "")
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
    }

    /// Handle known alerts through the action's minimum observation window.
    private func tapAlert(labels: [String], bound: TimeInterval, minimum: TimeInterval) throws {
        let springboard = XCUIApplication(bundleIdentifier: "com.apple.springboard")
        let started = ProcessInfo.processInfo.systemUptime
        let deadline = started + bound
        let floor = min(bound, minimum)
        let quiet: TimeInterval = 8
        var lastAlert = started
        var taps = 0
        var seen = "no alert"
        while ProcessInfo.processInfo.systemUptime < deadline {
            let alert = springboard.alerts.element
            if alert.exists {
                lastAlert = ProcessInfo.processInfo.systemUptime
                // The alert can go at any point between the look and the tap,
                // answered by its own timeout, by the system or by a person
                // at the phone, which leaves nothing to tap and nothing wrong.
                let options = XCTExpectedFailure.Options()
                options.isStrict = false
                XCTExpectFailure("the alert went before its tap", options: options) {
                    // The label is read before the tap, since the alert is
                    // gone after it.
                    let label = alert.label
                    seen = "the alert \(label) with \(alert.buttons.count) button(s)"
                    if let match = labels.first(where: { alert.buttons[$0].firstMatch.exists }) {
                        alert.buttons[match].firstMatch.tap()
                        taps += 1
                        print("tapped \(match) on the alert: \(label)")
                    }
                }
            }
            Thread.sleep(forTimeInterval: 0.5)
            let now = ProcessInfo.processInfo.systemUptime
            if now - started > floor, now - lastAlert > quiet {
                break
            }
        }
        if springboard.alerts.element.exists {
            throw TapperError.action("an alert stayed after \(Int(bound))s: \(seen)")
        }
        print(taps == 0 ? "no alert to tap" : "tapped \(taps) alert(s)")
    }
}
