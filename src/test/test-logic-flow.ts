import {
  unify,
  lvar,
  Subst,
  Term,
  walk,
  isVar,
  Var,
  Goal,
} from "../core.ts"
import {
  eq,
  disj,
  conj,
  and,
  or
} from "../relations.ts"
import { createLogicVarProxy } from "../run.ts";

const log = console.log;
const { proxy: $ } = createLogicVarProxy("test_");
const S = new Map();

const forceVar = (id: string) => ({
  tag: "var",
  id,
});

let outcnt = 0;
const outAll = async (format: Record<string, Term>, s: AsyncGenerator) => {
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

const add12 = (x) => x + 12;

const add12goal = (input, output) => {
  return async function* inner (s: Subst) {
    const fn = async () => {
      console.log("GOOO");
      const w = await walk(input, s);
      return add12(w);
    }
    const s2 = await unify(output, fn, s)
    if(s2) yield s2;
  }
}

const simple_eq = (t: Term, v: Term) => {
  return async function* inner (s: Subst) {
    const s2 = await unify(t, v, s);
    if(s2) yield s2;
  }
}

async function walkAllKeys<T extends Record<string, Term>>(
  obj: T,
  subst: Subst,
  vars = true,
): Promise<Record<string, Term>> {
  const result: Record<string, Term> = {};
  for (const key of Object.keys(obj)) {
    const v = await walk(obj[key], subst);
    if(vars || !isVar(v)) {
      result[key] = v;
    }
  }
  return result;
}



const xxx = lvar("xxx");

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
  // natStream($.y),
  // eq($.x, $.y),
);


await(outAll(
  {
    x: $.x,
    y: $.y,
  },
  goal(S)
));
