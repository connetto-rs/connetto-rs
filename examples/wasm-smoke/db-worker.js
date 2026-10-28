// A harness may load this script through a wrapper blob, so the query is read from import.meta.url.
const debug = new BroadcastChannel("connetto-debug");
const hello = new BroadcastChannel("connetto-hello");

const params = new URL(import.meta.url).searchParams;
// A failure before the Rust side starts has to name the boot the page is waiting for.
const boot = params.get("boot");
if (boot) {
  self.connettoBoot = boot;
}
const report = (stage) => {
  if (boot) {
    hello.postMessage("progress:" + boot + (stage ? ":" + stage : ""));
  }
};

report("bootstrap-started");
const beat = setInterval(() => report(), 5000);
try {
  const glue = params.get("glue");
  debug.postMessage("db worker: importing " + glue);
  const mod = await import(glue);
  report("module-imported");
  // The harness glue omits its default module path.
  await mod.default({ module_or_path: glue.replace(/\.js$/, "_bg.wasm") });
  clearInterval(beat);
  report("module-initialized");
  debug.postMessage("db worker: module ready, booting the db tier");
  await mod.db_worker_boot();
  debug.postMessage("db worker: serving");
} catch (err) {
  debug.postMessage("db worker FAILED: " + err);
  // A page cannot attribute a failure that names no boot.
  if (boot) {
    hello.postMessage("failed:" + boot + ":" + err);
  }
  throw err;
} finally {
  clearInterval(beat);
}
