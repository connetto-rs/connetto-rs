//! The bundled Kotlin module and btleplug's Java, and the virtual machine
//! btleplug's jni takes.

use manganis::android::with_activity;
use manganis::jni::JNIEnv;
use manganis::jni::errors::Error as JniError;

// Declared for `dx`, which builds and bundles the Kotlin module this names.
#[manganis::ffi("android")]
extern "Kotlin" {
    pub type HotspotPlugin;
    pub type BluetoothPlugin;
}

/// Why btleplug's virtual machine is not at hand.
#[derive(Debug, thiserror::Error)]
pub enum VmError {
    /// The host registered no Android context yet, so the virtual machine
    /// is unknown.
    #[error("no Android context is registered, so the virtual machine is unknown")]
    NoContext,
    /// The virtual machine refused a call.
    #[error("the virtual machine refused: {0}")]
    Jni(#[from] JniError),
}

/// The process's virtual machine under the jni btleplug links (R76 decision
/// 22), taken from the one manganis hands out.
///
/// # Errors
///
/// [`VmError::NoContext`] before the host registers its Android context, and
/// [`VmError::Jni`] when the virtual machine refuses the lookup.
pub fn java_vm() -> Result<jni::JavaVM, VmError> {
    let raw =
        with_activity(|env, _activity| Some(env.get_java_vm().map(|vm| vm.get_java_vm_pointer())))
            .ok_or(VmError::NoContext)??;
    #[expect(
        unsafe_code,
        reason = "jni 0.22 learns the running virtual machine only through from_raw"
    )]
    // SAFETY: `raw` is the process's one virtual machine, which manganis's jni
    // read from the live environment it handed out, so it is non-null and
    // stays valid for the life of the process. Both jni versions point at the
    // same `JNIInvokeInterface_` table.
    let vm = unsafe { jni::JavaVM::from_raw(raw.cast()) };
    Ok(vm)
}

/// Point the calling thread's context class loader at the application's, so
/// btleplug's class lookups on a thread attached from native code find the
/// bundled Java (R76 decision 22).
///
/// # Errors
///
/// The virtual machine's refusal of any of the framework calls.
pub fn use_application_class_loader(env: &mut jni::Env<'_>) -> jni::errors::Result<()> {
    use jni::{jni_sig, jni_str};
    let application = env
        .call_static_method(
            jni_str!("android/app/ActivityThread"),
            jni_str!("currentApplication"),
            jni_sig!("()Landroid/app/Application;"),
            &[],
        )?
        .l()?;
    let loader = env
        .call_method(
            &application,
            jni_str!("getClassLoader"),
            jni_sig!("()Ljava/lang/ClassLoader;"),
            &[],
        )?
        .l()?;
    let thread = env
        .call_static_method(
            jni_str!("java/lang/Thread"),
            jni_str!("currentThread"),
            jni_sig!("()Ljava/lang/Thread;"),
            &[],
        )?
        .l()?;
    env.call_method(
        &thread,
        jni_str!("setContextClassLoader"),
        jni_sig!("(Ljava/lang/ClassLoader;)V"),
        &[jni::objects::JValue::Object(&loader)],
    )?;
    Ok(())
}
