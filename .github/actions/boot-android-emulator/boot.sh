#!/usr/bin/env bash
# Boots emulator-5554 for the Android proof and waits for Android's boot to complete.
set -euo pipefail

logcat=$1
serial="emulator-5554"
image="system-images;android-35;google_apis;x86_64"
sdkmanager="$ANDROID_HOME/cmdline-tools/latest/bin/sdkmanager"
avdmanager="$ANDROID_HOME/cmdline-tools/latest/bin/avdmanager"
adb="$ANDROID_HOME/platform-tools/adb"

# The emi image carries every package, so its runs download nothing, which a
# corrupt download of the emulator once failed. A hosted runner fetches what it lacks.
missing=()
[ -x "$ANDROID_HOME/emulator/emulator" ] || missing+=(emulator)
[ -x "$adb" ] || missing+=(platform-tools)
[ -d "$ANDROID_HOME/system-images/android-35/google_apis/x86_64" ] || missing+=("$image")
if [ "${#missing[@]}" -gt 0 ]; then
  echo "installing ${missing[*]}"
  # A process substitution, because `yes | sdkmanager` ends `yes` with SIGPIPE, which pipefail turns into a failure.
  "$sdkmanager" --licenses < <(yes) >/dev/null
  "$sdkmanager" --install "${missing[@]}" >/dev/null
fi

export ANDROID_AVD_HOME="$HOME/.android/avd"
mkdir -p "$ANDROID_AVD_HOME"
echo "ANDROID_AVD_HOME=$ANDROID_AVD_HOME" >> "$GITHUB_ENV"
echo no | "$avdmanager" create avd --force --name proof --abi google_apis/x86_64 --package "$image" >/dev/null
# Two cores leave a cold Google APIs boot saturated for minutes, its System UI
# answering no input while the proof types the PIN.
printf 'hw.cpu.ncore=4\n' >> "$ANDROID_AVD_HOME/proof.avd/config.ini"

"$adb" start-server
nohup "$ANDROID_HOME/emulator/emulator" -port 5554 -avd proof -no-window \
  -gpu swiftshader_indirect -no-snapshot -noaudio -no-boot-anim \
  -logcat '*:v' -logcat-output "$logcat" > "$RUNNER_TEMP/emulator.log" 2>&1 &
emulator=$!

deadline=$((SECONDS + 600))
until [ "$(timeout 10 "$adb" -s "$serial" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" = 1 ]; do
  if ! kill -0 "$emulator" 2>/dev/null; then
    cat "$RUNNER_TEMP/emulator.log"
    echo "::error::the emulator exited before Android finished booting"
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    cat "$RUNNER_TEMP/emulator.log"
    echo "::error::Android did not finish booting within 600 s"
    exit 1
  fi
  sleep 2
done
echo "booted in ${SECONDS} s"

timeout 30 "$adb" -s "$serial" shell input keyevent 82
for setting in window_animation_scale transition_animation_scale animator_duration_scale; do
  timeout 30 "$adb" -s "$serial" shell settings put global "$setting" 0.0
done
