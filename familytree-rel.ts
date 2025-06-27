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

let parent_kid = (p: Term, k: Term): ProfilableGoal => {
  throw "must set parent_kid";
};
let relationship = (a: Term, b: Term): ProfilableGoal => {
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

// export const kidsAgg = groupAggregateRelFactory((v, k) => distinctVar(
//   k,
//   parent_kid(v, k),
// ),
// );

export const kidsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, anyKidOf(v, in_s), s);
});

// Refactored using generalized ancestorOf
export const grandparent_kid = (gp: Term, k: Term) => ancestorOf(2)(k,gp);
export const greatgrandparent_kid = (ggp: Term, k: Term) => ancestorOf(3)(k, ggp);

export const grandparentAgg = Rel((k, gp) => {
  const in_s = lvar("in_s");
  const { proxy: $$ } = createLogicVarProxy("grandparentagg_");
  // return distincto_G(
  //   gp, grandparent_kid(gp, k)
  // )
  return collecto($$.in_gp, grandparent_kid($$.in_gp, k), gp);
});

export const greatgrandparentAgg = Rel((k, gp) => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatgrandparent_kid(in_s, k), gp);
});

export const anyParentOf = Rel((v, p) => {
  const pp = lvar("anyparentof_parent");
  const sp = lvar("anyparentof_stepparent");
  return and(or(and(stepParentOf(v, p)), and(parentOf(v, p))));
});

export const anyKidOf = Rel((p, v) => {
  const pp = lvar("anyparentof_parent");
  const sp = lvar("anyparentof_stepparent");
  return and(or(and(stepKidOf(p, v)), and(parentOf(v, p))));
});

export const parentAgg = Rel(function parentAgg (k, p) {
  const in_s = lvar("in_s");
  return collecto(in_s, parentOf(k, in_s), p);
});

// Helper: succeeds if a is in a relationship with b or b with a
export const relationshipEitherWay = Rel((a: Term, b: Term) => {
  return or(relationship(a, b), relationship(b, a));
});

export const stepParentOf = Rel((kid: any, stepparent: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepparentof_");
  return and(
    parentOf(kid, $$.parent),
    relationshipEitherWay($$.parent, stepparent),
    not(parentOf(kid, stepparent)),
  );
});

export const stepKidOf = Rel((stepparent: any, kid: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepkidof_");
  return and(
    relationshipEitherWay(stepparent, $$.parent),
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
  const nonsharedparent_v = lvar("halfsibof_nonsharedparent_v");
  const sharedparent = lvar("halfsibof_sharedparent");
  const nonsharedparent_s = lvar("halfsibof_nonsharedparent_s");
  return and(
    parentOf(out_v, sharedparent),
    parentOf(out_s, sharedparent),

    parentOf(out_v, nonsharedparent_v),
    not(eq(nonsharedparent_v, sharedparent)),

    parentOf(out_s, nonsharedparent_s),
    not(eq(nonsharedparent_s, sharedparent)),

    not(eq(nonsharedparent_v, nonsharedparent_s)),

    not(eq(out_v, out_s)),
  );
});

export const halfSiblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, halfSiblingOf(v, in_s), s);
});

export const stepSiblingOf = Rel((out_v: any, out_s: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepsiblingof_");
  return and(
    parentOf(out_v, $$.vparent),
    relationshipEitherWay($$.vparent, $$.Mstepsibling_parent),
    parentOf(out_s, $$.Mstepsibling_parent),
    neq_C(out_v, out_s),
    // tap("SSSS"),
    not(parentOf(out_v, $$.Mstepsibling_parent)),
    not(parentOf(out_s, $$.vparent)),
  );
});

export const stepSiblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, stepSiblingOf(v, in_s), s);
});

export const siblingOf = Rel((v, s) =>
  or(fullSiblingOf(v, s), halfSiblingOf(v, s), stepSiblingOf(v, s)),
);

export const siblingsAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, siblingOf(v, in_s), s);
});

// Refactored using uncleOfLevel
export const uncleOf = uncleOfLevel(1);
export const greatuncleOf = uncleOfLevel(2);

export const uncleAgg = Rel((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, uncleOf(v, in_s), s);
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
        or(eq(uncle, sibling), relationshipEitherWay(sibling, uncle)),
        // Exclude direct ancestors
        not(anyParentOf(person, uncle)),
      ),
    );
  });
}

// Exclude if candidate is a sibling of any ancestor of person up to maxLevel
function isSiblingOfAnyAncestor(person: any, candidate: any, maxLevel: number) {
  const goals = [];
  for (let i = 1; i < maxLevel; ++i) {
    const anc = lvar(`anc_${i}`);
    goals.push(and(ancestorOf(i)(person, anc), siblingOf(anc, candidate)));
  }
  return or(...goals);
}

// Strict cousin relationship: ensure closest common ancestor is at the correct level
function cousinOf(a: any, b: any, degree = 1, removal = 0): any {
  if (degree < 1) return eq(0, 1);
  let levelA, levelB;
  if (removal < 0) {
    levelA = degree + 1 - removal;
    levelB = degree + 1;
  } else {
    levelA = degree + 1;
    levelB = degree + 1 + removal;
  }
  const ancestorA = lvar("cousinGen_ancestorA");
  const ancestorB = lvar("cousinGen_ancestorB");
  // Constraints to ensure no closer common ancestor
  const noCloserCommon = [];
  for (let i = 1; i < levelA; ++i) {
    const closerA = lvar(`closerA_${i}`);
    for (let j = 1; j < levelB; ++j) {
      const closerB = lvar(`closerB_${j}`);
      noCloserCommon.push(
        not(
          and(
            ancestorOf(i)(a, closerA),
            ancestorOf(j)(b, closerB),
            eq(closerA, closerB),
          ),
        ),
      );
    }
  }
  return distincto_G(
    b,
    and(
      ancestorOf(levelA)(a, ancestorA),
      ancestorOf(levelB)(b, ancestorB),
      eq(ancestorA, ancestorB),
      ...noCloserCommon,
      // not(eq(a, b)),
      // not(siblingOf(a, b)),
      // not(anyParentOf(a, b)),
      // not(anyParentOf(b, a)),
      not(isSiblingOfAnyAncestor(a, b, Math.max(levelA, levelB))),
      not(isSiblingOfAnyAncestor(b, a, Math.max(levelA, levelB))),
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
