import * as L from "./logic_lib.ts";
const taps = (msg: string) =>
    async function* (s: L.Subst) {
        console.log("TAP", msg, s);
        yield s;
    };

for await (const row of L.runEasy(($) => [
    { x: $.x, z: $.z },
    L.all(
        taps("start"),
        L.membero($.x, [1, 2, 3]),
        taps("set x"),
        L.mapInlineLazy(async (z) => z + 2, $.x, $.y),
        taps("set y"),
        L.eq($.y, $.z),
        taps("set z"),

    )
])) {
    console.log(row)
}