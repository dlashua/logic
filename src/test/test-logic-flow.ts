import { Subst, Term, walk } from "../core.ts"
import { eq, and, or } from "../relations.ts"
import { createLogicVarProxy } from "../run.ts";

const log = console.log;
const { proxy: $ } = createLogicVarProxy("test_");
const S = new Map();

const forceVar = (id: string) => ({
  tag: "var",
  id,
});

let outcnt = 0;
const outAll = async (format: Record<string, Term>, s: AsyncGenerator<Subst>) => {
  const myOutCnt = ++outcnt;
  let cnt = 0;
  for await (const one of s) {
    const t: Record<string, any> = {};
    t.__s__ = one;
    // t.__d__ = one;
    for (const [k,v] of Object.entries(format)) {
      t[k] = await walk(v, one)
    }

    console.log(myOutCnt, ++cnt, t);
  }
}

const goal = and(
  or(
    eq($.x, 20),
    eq($.x, 3),
    eq($.x, 4),
  ),
  or(
    eq($.y, 7),
    eq($.y, 3),
    eq($.y, 20),
  ),
  eq($.x,$.y),
);


await(outAll(
  {
    x: $.x,
    y: $.y,
  },
  goal(S)
));
