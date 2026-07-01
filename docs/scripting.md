# Scripting with Rhai

The built-in stages (`filter`, `sample`, `aggregate`, `project`) cover the shapes most routes need, and
they are the right tool when they fit: they compile to fixed forms and cost nothing per message beyond
the work they do. But telemetry logic is open-ended — a derived engineering unit, a reading you only
care about when it moves, a vendor payload that doesn't match the southbound shape, a decision that
depends on the *relationship* between several samples. **Scripting is the escape hatch for logic the
built-ins don't express.**

The processor embeds [Rhai](https://rhai.rs) — a small, sandboxed, Rust-native scripting language. A
script is compiled **once at startup** and then run per message on the route's hot path. It has no I/O,
no access to the filesystem or network, and runs under a bounded operation budget, so a script can shape
data but cannot reach outside the pipeline or wedge a worker.

This page is the complete guide: the two places scripts run, everything a script can see, what it must
return, the language features you'll actually use, how array-typed values behave, and a cookbook of
worked examples — each one explained (goal → how → why), and each one backed by a test in the
processor's own suite so the syntax is known-good.

> **New to a specific task?** For *where* scripts live (inline vs. an external `.rhai` file) and how to
> ship them to a device or pod, see [Use an external script file](how-to-guides.md#use-an-external-script-file)
> and [Ship script files with a deployment](how-to-guides.md#ship-script-files-with-a-deployment). For
> the config field reference, see [`script`](reference/configuration.md#script-stage).

## The two places a script runs

Scripting appears in two roles. Both use the same engine and the same [scope](#the-scope-what-a-script-sees);
they differ only in what they return.

- A **`filter` `script`** is a **predicate**: it evaluates to a boolean, and the message is kept when the
  result is `true`. Use it when the built-in `field`/`op`/`value` or `quality` filters can't express the
  condition — e.g. a test that spans several samples or inspects an array.

  ```jsonc
  { "filter": { "script": "value.filter(|x| x > 50).len() >= 2" } }
  ```

- A **`script` stage** is a **transform**: it evaluates to the **new message body** (a map), or to `()`
  (Rhai's unit value) to **drop** the message. Use it to derive fields, reshape a payload, or conditionally
  discard.

  ```jsonc
  { "script": "#{ \"tempF\": value * 1.8 + 32.0 }" }
  ```

Inline source is fine for a one-liner. Anything longer belongs in a `.rhai` file referenced as
`{"script": {"file": "rules/derive.rhai"}}`, which version-controls cleanly, needs no JSON string-escaping,
and is compiled and validated at startup — a missing file or a syntax error stops the component
immediately rather than failing silently at the first message.

## The scope: what a script sees

Before each evaluation the stage builds a fresh **scope** and binds two groups of variables: the
**message view** (the data in front of you) and the **runtime context** (constant facts about where the
script is running). Everything is a plain value — read them, combine them, return something new.

### The message view

| Binding | Type | What it is |
|---|---|---|
| `topic` | string | the source topic the message arrived on |
| `body` | map | the full message body — `body.signal`, `body.samples`, `body.device`, or *any* JSON your payload carries |
| `tags` | map | the message-**envelope** metadata (`tags.site`, `tags.thing`, …) — identity, **not** the signal |
| `samples` | array | `body.samples` (or `[]` when absent); each element is a map with `value`, `quality`, `sourceTs`, … |
| `value` | any | convenience: the **first** sample's `value` (a number, string, bool, **or array**) |
| `quality` | string | convenience: the **first** sample's `quality` (`""` when absent) |

`body` and `tags` are the two roots; `value`/`quality`/`samples` are conveniences derived from the
southbound shape. On a payload that *isn't* southbound-shaped, `samples` is `[]` and `value`/`quality`
are empty — but `body` always holds whatever arrived, so **a script is payload-agnostic**: read your own
paths off `body`.

### The runtime context

These are **constant for the life of a route** — the processor's own identity and which route is
running. They mirror the config template variables (`{ThingName}` etc.), and they exist so a **single
generic script can be reused across components** and still stamp or branch on where it runs.

| Binding | Type | What it is |
|---|---|---|
| `thingName` | string | the IoT Thing name (`{ThingName}`) |
| `componentName` | string | the short component name (`{ComponentName}`, the segment after the last `.`) |
| `componentFullName` | string | the fully-qualified component name (`{ComponentFullName}`) |
| `routeId` | string | the id of the route this script belongs to |
| `recvMs` | integer | the broker **receive time** of this message, in Unix milliseconds |

`recvMs` is per-message (it changes each call); the four identity bindings are the same for every message
on the route.

## What a script returns

The return value is the entire contract — there is no other output channel, and a script never mutates
the message in place.

- A **`filter` `script`** returns a **boolean**. `true` keeps the message, `false` drops it. A
  non-boolean result, or a runtime error, is treated as `false` (drop) and logged at WARN — **a filter
  fails closed**, so a broken predicate discards rather than leaks.
- A **`script` stage** returns the **new body**. A map (`#{ … }`) replaces `body`; the envelope
  (`header`, `tags`) is preserved. Returning **`()`** drops the message. A result that can't convert to
  JSON, or a runtime error, also drops the message (logged at WARN).

## Scripts are stateless

Each evaluation gets a **fresh scope** and sees **only the current message** — there is no variable that
survives from one message to the next, no counter, no rolling buffer inside a script. This is deliberate:
a script stays a pure function of its input, which makes it easy to reason about, impossible to leak
memory across messages, and safe to share one engine across every route.

**Cross-message state lives in the built-in stages**, which the route worker owns and drives on a timer:
use `sample` for per-key rate limiting and `aggregate` for windowed counters, min/max/avg. A common,
powerful pattern is to let a script *shape* each message and an `aggregate` stage *accumulate* across
them — see the cookbook's [rate-of-change](#3-rate-of-change-across-consecutive-samples) example, which a
downstream `aggregate` can then reduce.

## A Rhai primer for the processor

You don't need to learn all of Rhai. This is the subset that matters here; the [Rhai
book](https://rhai.rs/book/) has the full language.

**Values.** Integers (`42`), floats (`3.14`), strings (`"ok"`), booleans, arrays (`[1, 2, 3]`), object
maps (`#{ "a": 1 }`), and `()` (unit — "nothing", used to drop a message). JSON from the message maps
directly: a JSON object → a Rhai map, a JSON array → a Rhai array, a JSON number → an integer or float.

**Maps and arrays.** Build a body with a map literal `#{ "k": v, … }`; build a list with `[a, b, …]`.
Read fields with `.` (`body.signal.id`) and index with `[]` (`samples[0].value`).

**Math.** The usual `+ - * / %`. Integers and floats mix (an integer is promoted), so `value * 1.8`
works whether `value` is `20` or `20.0`. Float methods like `.sqrt()`, `.round()`, `.abs()` are
available.

**Control flow.** `if cond { … } else { … }`; `switch x { "A" => 1, _ => 0 }` for value dispatch;
`for x in xs { … }` and `for i in 0..n { … }` (ranges are exclusive of the end); `while cond { … }`. Use
`return v;` to exit a function or the script early.

**Functions.** Define reusable helpers with `fn name(args) { … }` — the last expression is the return
value. Functions keep a script readable when the logic is more than a line.

**Array methods.** `len()`, `is_empty()`, `push(x)`, and the higher-order trio you'll reach for
constantly: `map(|x| …)` (transform each element), `filter(|x| …)` (keep matching elements), and
`reduce(|acc, x| …, seed)` (fold to a single value). Also `all(|x| …)`, `some(|x| …)`, `contains(x)`,
`index_of(x)`.

**The operation budget.** Every evaluation runs under a cap of **1,000,000 operations**, so a runaway
loop can't hang a worker — but it also means a script that loops over a very large array on every message
has a real cost. Keep scripts tight; push heavy accumulation into `aggregate`.

## Working with array-typed values

A sample's `value` is not always a scalar. An OPC UA array node, a batched register read, or a vector
signal produces `value` as a **JSON array**, and the wire format carries it faithfully. In a script that
array arrives as an ordinary **Rhai array**, so all the array machinery above applies — iterate it,
`map`/`filter`/`reduce` it, index it. The [cookbook](#2-array-node-mean-peak-and-rms) shows mean, peak,
and RMS over an array value.

Array values are first-class in the built-ins too: the **`aggregate` stage folds an array value
element-wise** (every element counts and feeds `avg`/`min`/`max`/`sum`), and a **filter/key path can
flatten an array** with a trailing `[]` (`body.samples[].value[]`). For archiving, the file sink's
default projection stores an array as a JSON string in `valueString`, and a
[declared `rows` projection](reference/data-types.md#rows-user-projection) can `explode` an array into one
row per element. So an array signal flows through filtering, scripting, aggregation, and archival without
being dropped or flattened to an opaque blob.

## Cookbook

Real patterns, each with the goal, the script, how it works, and why you'd write it this way. Every
example here is exercised by a test in `src/proc/script.rs`, so the syntax is verified.

### 1. Derive an engineering unit, dropping empty reads

**Goal:** convert a raw Celsius reading to Fahrenheit, but drop a message that carries no sample rather
than emitting a bogus value.

```rhai
fn to_fahrenheit(c) { c * 1.8 + 32.0 }

if samples.is_empty() { return (); }        // no reading → drop the message

#{ "signal": body.signal, "tempF": to_fahrenheit(value) }
```

**How it works.** A helper `fn` names the conversion so the intent is obvious and the formula lives in
one place. The guard runs first: if there are no samples, `return ()` drops the message before anything
tries to use the (absent) `value`. Otherwise the script returns a new body that keeps the source
`signal` identity and adds the derived `tempF`.

**Why.** Deriving units at the edge means the cloud stores query-ready values, not raw counts. The guard
matters because `value` is `()` when `samples` is empty — computing on it would produce a garbage row;
dropping is the honest outcome.

### 2. Array node: mean, peak, and RMS

**Goal:** an OPC UA array node delivers `value` as `[10.0, 20.0, 30.0]`; emit summary statistics for it.

```rhai
fn mean(xs) {
    if xs.is_empty() { return 0.0; }
    let s = 0.0;
    for x in xs { s += x; }
    s / xs.len()
}

let readings = value;                         // the array value
let peak = readings[0];
for x in readings { if x > peak { peak = x; } }

#{ "mean": mean(readings), "peak": peak, "n": readings.len() }
```

RMS (root-mean-square) is the same shape with a `.sqrt()`:

```rhai
let sumsq = 0.0;
for x in value { sumsq += x * x; }
#{ "rms": (sumsq / value.len()).sqrt() }
```

**How it works.** Because `value` is an array, `for x in value` iterates its elements, `readings[0]`
indexes it, and `readings.len()` counts them — exactly as you'd expect for a list. `mean` is factored
into a helper; `peak` is a running maximum; RMS accumulates squares then takes the root with the float
`.sqrt()` method.

**Why.** Shipping the raw array to the cloud pushes the reduction downstream and multiplies storage. Computing
mean/peak/RMS at the edge turns a burst of numbers into the few figures an operator actually watches.

### 3. Rate of change across consecutive samples

**Goal:** a batched message carries several samples; emit the deltas between consecutive readings.

```rhai
let deltas = [];
for i in 1..samples.len() {
    deltas.push(samples[i].value - samples[i - 1].value);
}
#{ "deltas": deltas }
```

**How it works.** The range `1..samples.len()` walks the sample indices from the second to the last;
each step subtracts the previous sample's value from the current one and appends the difference. The
built-in filters can't do this — they test one value at a time and have no notion of "the previous
sample".

**Why.** Rate of change often matters more than the absolute value — a temperature *climbing* fast is the
alarm, not the temperature itself. Emitting deltas lets a downstream `aggregate` (max delta per window)
or `filter` catch it.

### 4. Keep only when an array crosses a threshold enough times

**Goal:** a `filter` that keeps a message only when **at least two** elements of an array value exceed
50.

```rhai
value.filter(|x| x > 50).len() >= 2
```

**How it works.** `value.filter(|x| x > 50)` produces the sub-array of over-threshold elements; `.len()`
counts them; `>= 2` is the boolean the filter needs. One expression, no loop.

**Why.** A single spike might be noise; two or more elements over the line is a signal. Expressing
"how many crossed" is exactly what the scalar built-in filters can't do.

### 5. A generic, reusable script that stamps identity

**Goal:** one script, deployed to many components, that tags each message with where it came from and
which route handled it — without hard-coding those values.

```rhai
#{
    "signal": body.signal,
    "value": value,
    "thing": thingName,
    "component": componentName,
    "route": routeId,
    "ingestedMs": recvMs
}
```

**How it works.** `thingName`, `componentName`, `routeId`, and `recvMs` come from the [runtime
context](#the-runtime-context), not the message — so the *same* script text produces
`"thing": "edge-42"` on one device and `"thing": "edge-77"` on another. `recvMs` records when the
processor received the message.

**Why.** Reusable scripts are how you avoid a bespoke script per component. Because identity is injected
rather than written into the script, one file in a ConfigMap or artifact bundle can serve a whole fleet.

### 6. Normalize a non-southbound vendor payload

**Goal:** an upstream publisher sends `{"dev": "pump-7", "metric": "vibration", "raw": 325}`; reshape it
into the southbound `signal` shape so the built-in stages and the file sink's default projection work.

```rhai
#{
    "signal": #{ "id": body.dev, "name": body.metric },
    "samples": [ #{ "value": body.raw * 0.1, "quality": "GOOD" } ]
}
```

**How it works.** The script reads the vendor fields straight off `body` and constructs a proper
`SouthboundSignalUpdate`-shaped body: a `signal` map with `id`/`name`, and a one-element `samples` array
carrying the scaled value. Nested map literals build the structure inline.

**Why.** This is the adapter-of-last-resort: rather than teach every downstream stage a new payload
shape, one `script` stage at the front of the route normalizes into the shape everything already
understands. It's the payload-agnostic model in action.

### 7. Map a status string to a code

**Goal:** translate a vendor status string into a compact numeric code.

```rhai
let code = switch body.status {
    "RUNNING" => 1,
    "IDLE" => 0,
    "FAULT" => -1,
    _ => 99,
};
#{ "statusCode": code }
```

**How it works.** `switch` dispatches on the value of `body.status`; each arm maps a known string to its
code, and `_` catches anything unexpected (here, `99`). The result is bound to `code` and returned in the
new body.

**Why.** Numeric codes are cheaper to store and easier to threshold/alarm on than free-text status
strings, and the `_` arm makes "unknown" explicit instead of silently dropping.

### 8. Sum an array with `reduce`

**Goal:** total an array value in one expression instead of a loop.

```rhai
#{ "total": value.reduce(|a, v| a + v, 0.0) }
```

**How it works.** `reduce` folds the array: it starts the accumulator `a` at the seed `0.0`, then calls
`|a, v| a + v` for each element, threading the running total through. The final accumulator is the sum.

**Why.** For a simple fold, `reduce` is clearer than a `for` loop with an external accumulator, and it
composes — swap the closure for `if v > a { v } else { a }` and you have `max` instead.

## Limits and gotchas

- **Stateless.** No memory across messages; use `sample`/`aggregate` for cross-message state.
- **Fail-closed / drop-on-error.** A filter that errors drops the message; a transform that errors or
  returns non-JSON drops it (both logged at WARN). Scripts never crash the route.
- **The 1,000,000-op budget** bounds each evaluation — deep loops over large arrays on every message add
  up; push heavy work into `aggregate`.
- **No I/O.** Scripts can't read files, call the network, or see other messages — by design.
- **Integer vs. float.** JSON `20` arrives as an integer and `20.0` as a float; mixed arithmetic promotes
  to float, but if you need a float result from integer inputs, multiply by a float (`x * 1.0`).
- **Compiled once at startup.** Editing a `.rhai` file needs a restart (a new deployment / pod rollout),
  not a live reload.

## See also

- [Use an external script file](how-to-guides.md#use-an-external-script-file) — inline vs. file, `scriptsDir`.
- [Ship script files with a deployment](how-to-guides.md#ship-script-files-with-a-deployment) — Greengrass artifacts / k8s ConfigMap.
- [`script` configuration reference](reference/configuration.md#script-stage) — the config fields.
- [Explanation — the pipeline](explanation.md) — where scripting sits among the stages.
