import { Term, lvar } from "./core.ts";
import { collecto } from "./relations-agg.ts";
import {
  Goal,
  Rel,
  and,
  distincto_G,
  eq,
  neq_C,
  not,
  or,
} from "./relations.ts";
import { createLogicVarProxy } from "./run.ts";
import type { ProfiledGoal, ProfilableGoal } from "./relations.ts";

let parent_kid = (p: Term, k: Term): Goal => {
  throw "must set parent_kid";
};
let relationship = (a: Term<string|number>, b: Term<string|number>): Goal => {
  throw "must set relationship";
};

export function set_parent_kid(fn: typeof parent_kid) {
  parent_kid = Rel(fn);
}

export function set_relationship(fn: typeof relationship) {
  relationship = Rel(fn);
}

export const parentOf = Rel((v, p) => parent_kid(p, v));

export const person = Rel((p) => {
  const { proxy: $$ } = createLogicVarProxy("person_");
  return distincto_G(
    p,
    or(parent_kid(p, $$.kid), parent_kid($$.parent, p)),
  );
});

export const kidsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, anyKidOf(v, in_s), s);
});

// Refactored using generalized ancestorOf
export const grandparent_kid = (gp: Term, k: Term) => ancestorOf(2)(k,gp);
export const greatgrandparent_kid = (ggp: Term, k: Term) => ancestorOf(3)(k, ggp);

export const grandparentAgg = Rel((k, gp) => {
  const { proxy: $$ } = createLogicVarProxy("grandparentagg_");
  return collecto($$.in_gp, grandparent_kid($$.in_gp, k), gp);
});

export const greatgrandparentAgg = Rel((k, gp) => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatgrandparent_kid(in_s, k), gp);
});

export const anyParentOf = Rel((v, p) => {
  return or(stepParentOf(v, p), parentOf(v, p));
});

export const anyKidOf = Rel((p, v) => {
  return or(stepKidOf(p, v), parentOf(v, p));
});

export const kidOf = Rel((p, v) => {
  return parentOf(v, p);
});

export const parentAgg = Rel(function parentAgg (k, p) {
  const in_s = lvar("in_s");
  return collecto(in_s, parentOf(k, in_s), p);
});

export const stepParentOf = Rel((kid: any, stepparent: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepparentof_");
  return and(
    parentOf(kid, $$.parent),
    relationship($$.parent, stepparent),
    not(parentOf(kid, stepparent)),
  );
});

export const stepKidOf = Rel((stepparent: any, kid: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepkidof_");
  return and(
    relationship(stepparent, $$.parent),
    parentOf(kid, $$.parent),
    not(parentOf(kid, stepparent)),
  );
});

export const stepParentAgg = Rel((k, p) => {
  const in_s = lvar("in_s");
  return collecto(in_s, stepParentOf(k, in_s), p);
});

export const tap = (msg: any) => {
  return function* (s: any) {
    console.log(msg, s);
    yield s;
  };
};

export const fullSiblingOf = Rel((out_v, out_s) => {
  const p1 = lvar("fullsibof_p1");
  const p2 = lvar("fullsibof_p2");
  return distincto_G(
    out_s,
    and(
      parentOf(out_v, p1),
      parentOf(out_v, p2),

      parentOf(out_s, p1),
      parentOf(out_s, p2),

      not(eq(p1, p2)),
      not(eq(out_v, out_s)),
    ),
  );
});

export const fullSiblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, fullSiblingOf(v, in_s), s);
});

export const halfSiblingOf = Rel((out_v, out_s) => {
  const sharedparent = lvar("halfsibof_sharedparent");
  return distincto_G(
    out_s,
    and(
      parentOf(out_v, sharedparent),
      parentOf(out_s, sharedparent),
      not(eq(out_v, out_s)),
      not(fullSiblingOf(out_v, out_s)),
    )
  )
});

export const halfSiblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, halfSiblingOf(v, in_s), s);
});

export const stepSiblingOf = Rel((out_v: any, out_s: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepsiblingof_");
  return distincto_G(
    out_s,
    and(
      anyParentOf(out_v, $$.parent),
      anyKidOf($$.parent, out_s),
      not(eq(out_v, out_s)),
      not(halfSiblingOf(out_v, out_s)),
      not(fullSiblingOf(out_v, out_s)),
    )
  )
});

export const stepSiblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, stepSiblingOf(v, in_s), s);
});

export const siblingOf = Rel((v, s) => {
  const in_s = lvar("in_s");
  return distincto_G(s, and(anyParentOf(v, in_s), anyKidOf(in_s, s), not(eq(v, s))))
});

export const siblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, siblingOf(v, in_s), s);
});

// Refactored using uncleOfLevel
export const uncleOf = uncleOfLevel(1);
export const greatuncleOf = uncleOfLevel(2);

export const uncleAgg = Rel((v, s, level = 1) => {
  const in_s = lvar("in_s");
  return collecto(in_s, uncleOfLevel(level)(v, in_s), s);
});

export const greatuncleAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatuncleOf(v, in_s), s);
});

// Generalized ancestor relation: ancestorOf(level)(descendant, ancestor)
export const ancestorOf = function ancestorOf(level: number) {
  return Rel((descendant, ancestor) => {
    if (level < 1) return eq(descendant, ancestor);
    const chain = [descendant];
    for (let i = 0; i < level; ++i) {
      chain.push(lvar(`ancestor_${i}`));
    }
    const goals = [];
    for (let i = 0; i < level; ++i) {
      goals.push(anyParentOf(chain[i], chain[i + 1]));
    }
    goals.push(eq(chain[level], ancestor));
    return and(...goals);
  });
};


// Generalized uncle/aunt relation: uncleOfLevel(level)(person, uncle)
export function uncleOfLevel(level = 1) {
  return Rel((person: any, uncle: any) => {
    const ancestor = lvar("uncle_ancestor");
    const sibling = lvar("uncle_sibling");
    return distincto_G(
      uncle,
      and(
        ancestorOf(level)(person, ancestor),
        siblingOf(ancestor, sibling),
        // uncle can be sibling or sibling-in-law
        or(eq(uncle, sibling), relationship(sibling, uncle)),
        // Exclude direct ancestors
        not(anyParentOf(person, uncle)),
      ),
    );
  });
}

// Classic cousinOf: climb up degree steps from a to ancestor, then down degree-removal steps to b
export function cousinOf(a: any, b: any, degree = 1, removal = 0): any {
  if (degree < 1) return eq(0, 1); // invalid
  const upR = removal > 0 ? removal : 0;
  const downR = removal < 0 ? (removal * -1) : 0;
  const stepsUp = degree + 1 + upR;
  const stepsDown = degree + 1 + downR;
  if (stepsDown < 1) return eq(0, 1); // invalid

  const exclusions = [];
  const commonAncestor = lvar("cousinOf_commonAncestor");

  let prevA = a;
  const aUpGoals = [];
  const aUp = [a];
  for (let i = 1; i <= stepsUp; ++i) {
    const anc = (i === stepsUp) ? commonAncestor : lvar(`cousinOf_a_anc_${i}`);
    aUp.push(anc);
    aUpGoals.push(anyParentOf(prevA, anc));
    prevA = anc;
  }

  let prevB = commonAncestor;
  const bDownGoals = [];
  for (let i = 1; i <= stepsDown; ++i) {
    const kid = (i === stepsDown) ? b : lvar(`cousinOf_b_down_${i}`);
    bDownGoals.push(anyKidOf(prevB, kid));
    const ancestorLevel = stepsUp - i;
    if(ancestorLevel !== stepsUp && ancestorLevel > 0) {
      bDownGoals.push(not(eq(kid, aUp[ancestorLevel])));
    }
    prevB = kid;
  }

  exclusions.push(not(eq(a, b))); 
  
  return distincto_G(
    b,
    and(
      ...aUpGoals,
      ...bDownGoals,
      ...exclusions,
    ),
  );
}

export const cousinsAgg = Rel((v, s, degree = 1, removal = 0) => {
  const in_s = lvar("in_s");
  return collecto(in_s, cousinOf(v, in_s, degree, removal), s);
});

export const firstcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 1);
export const secondcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 2);
export const thirdcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 3);

// Nephew/Niece relation: nephewOf(person, nephew)
export const nephewOf = Rel((person, nephew) => {
  const parent = lvar("nephew_parent");
  return distincto_G(
    nephew,
    and(
      anyParentOf(nephew, parent),
      siblingOf(person, parent),
      not(eq(person, nephew)),
    ),
  );
});
