import {
  cons,
  isCons,
  isNil,
  lvar,
  nil,
  unify,
  walk
} from "./core.ts"
import type { Term } from "./core.ts";
import {
  and,
  eq,
  Goal,
  maybeProfile,
  ProfilableGoal
} from "./relations.ts"

export function membero(x: Term, list: Term): ProfilableGoal {
  return maybeProfile(async function* (s) {
    const l = await walk(list, s);
    if (l && typeof l === "object" && "tag" in l) {
      if ((l as any).tag === "cons") {
        const s1 = await unify(x, (l as any).head, s);
        if (s1) yield s1;
        for await (const s2 of membero(x, (l as any).tail)(s)) yield s2;
      }
    } else if (Array.isArray(l)) {
      for (const item of l) {
        const walkedItem = await walk(item, s);
        const s2 = await unify(x, walkedItem, s);
        if (s2) yield s2;
      }
    }
  });
}

export function firsto(x: Term, xs: Term): Goal {
  return async function* (s) {
    const l = await walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = await unify(x, consNode.head, s);
      if (s1) yield s1;
    }
  };
}

export function resto(xs: Term, tail: Term): Goal {
  return async function* (s) {
    const l = await walk(xs, s);
    if (isCons(l)) {
      const consNode = l as { tag: "cons"; head: Term; tail: Term };
      const s1 = await unify(tail, consNode.tail, s);
      if (s1) yield s1;
    }
  };
}

export function appendo(xs: Term, ys: Term, zs: Term): Goal {
  return async function* (s) {
    const xsVal = await walk(xs, s);
    if (isCons(xsVal)) {
      const consNode = xsVal as { tag: "cons"; head: Term; tail: Term };
      const head = consNode.head;
      const tail = consNode.tail;
      const rest = lvar();
      const s1 = await unify(
        zs,
        {
          tag: "cons",
          head,
          tail: rest,
        },
        s,
      );
      if (s1) {
        for await (const s2 of appendo(tail, ys, rest)(s1)) yield s2;
      }
    } else if (isNil(xsVal)) {
      const s1 = await unify(ys, zs, s);
      if (s1) yield s1;
    }
  };
}

function logicListToArray(list: Term): Term[] {
  const out = [];
  let cur = list;
  while (
    cur &&
    typeof cur === "object" &&
    "tag" in cur &&
    (cur as any).tag === "cons"
  ) {
    out.push((cur as any).head);
    cur = (cur as any).tail;
  }
  return out;
}

export function permuteo(xs: Term, ys: Term): Goal {
  return async function* (s) {
    const xsVal = walk(xs, s);
    if (isNil(xsVal)) {
      yield* eq(ys, nil)(s);
      return;
    }
    if (isCons(xsVal)) {
      const arr = logicListToArray(xsVal);
      for (const head of arr) {
        const rest = lvar();
        for await (const s1 of and(
          removeFirsto(xsVal, head, rest),
          permuteo(rest, lvar()),
          eq(ys, cons(head, lvar())),
        )(s)) {
          const ysVal2 = walk(ys, s1);
          if (isCons(ysVal2)) {
            for await (const s2 of eq(ysVal2.tail, walk(lvar(), s1))(s1)) {
              yield s2;
            }
          }
        }
      }
    }
  };
}

export function mapo(
  rel: (x: Term, y: Term) => Goal,
  xs: Term,
  ys: Term,
): Goal {
  return async function* (s) {
    const xsVal = walk(xs, s);
    const ysVal = walk(ys, s);
    if (isNil(xsVal)) {
      yield* eq(ys, nil)(s);
      return;
    }
    if (isCons(xsVal)) {
      const xHead = xsVal.head;
      const xTail = xsVal.tail;
      const yHead = lvar();
      const yTail = lvar();
      for await (const s1 of and(
        eq(ys, cons(yHead, yTail)),
        rel(xHead, yHead),
        mapo(rel, xTail, yTail),
      )(s)) {
        yield s1;
      }
    }
  };
}

export function removeFirsto(xs: Term, x: Term, ys: Term): Goal {
  return async function* (s) {
    const xsVal = await walk(xs, s);
    if (isNil(xsVal)) {
      yield* eq(ys, nil)(s);
      return;
    }
    if (isCons(xsVal)) {
      if (xsVal.head === x) {
        yield* eq(ys, xsVal.tail)(s);
      } else {
        const rest = lvar();
        for await (const s1 of and(
          eq(ys, cons(xsVal.head, rest)),
          removeFirsto(xsVal.tail, x, rest),
        )(s)) {
          yield s1;
        }
      }
    }
  };
}
