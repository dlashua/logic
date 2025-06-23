import * as L from "./logic_lib.ts";
import { distincto_G, neq_C } from "./relations.ts";

let parent_kid = (p: L.Term, k: L.Term): L.Goal => {
  throw "must set parent_kid";
};
let relationship = (a: L.Term, b: L.Term): L.Goal => {
  throw "must set relationship";
};

export function set_parent_kid(fn: typeof parent_kid) {
  parent_kid = fn;
}

export function set_relationship(fn: typeof relationship) {
  relationship = fn;
}

export const parentOf = L.Rel((v, p) => parent_kid(p, v));

export const person = L.Rel((p) => {
  const $$ = L.createLogicVarProxy(undefined, "person_");
  return L.distincto_G(
    p,
    L.or(parent_kid(p, $$.kid), parent_kid($$.parent, p)),
  );
});

// export const kidsAgg = L.groupAggregateRelFactory((v, k) => L.distinctVar(
//   k,
//   parent_kid(v, k),
// ),
// );

export const kidsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, anyKidOf(v, in_s), s);
});

// Refactored using generalized ancestorOf
export const grandparent_kid = ancestorOf(2);
export const greatgrandparent_kid = ancestorOf(3);

export const grandparentAgg = L.Rel((k, gp) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, grandparent_kid(in_s, k), gp);
});

export const greatgrandparentAgg = L.Rel((k, gp) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, greatgrandparent_kid(in_s, k), gp);
});

export const anyParentOf = L.Rel((v, p) => {
  const pp = L.lvar("anyparentof_parent");
  const sp = L.lvar("anyparentof_stepparent");
  return L.and(L.or(L.and(stepParentOf(v, p)), L.and(parentOf(v, p))));
});

export const anyKidOf = L.Rel((p, v) => {
  const pp = L.lvar("anyparentof_parent");
  const sp = L.lvar("anyparentof_stepparent");
  return L.and(L.or(L.and(stepKidOf(p, v)), L.and(parentOf(v, p))));
});

export const parentAgg = L.Rel((k, p) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, parentOf(k, in_s), p);
});

// Helper: succeeds if a is in a relationship with b or b with a
export const relationshipEitherWay = L.Rel((a: L.Term, b: L.Term) => {
  return L.or(relationship(a, b), relationship(b, a));
});

export const stepParentOf = L.Rel((kid: any, stepparent: any) => {
  const $$ = L.createLogicVarProxy(undefined, "stepparentof_");
  return L.and(
    parentOf(kid, $$.parent),
    relationshipEitherWay($$.parent, stepparent),
    L.not(parentOf(kid, stepparent)),
  );
});

export const stepKidOf = L.Rel((stepparent: any, kid: any) => {
  const $$ = L.createLogicVarProxy(undefined, "stepkidof_");
  return L.and(
    relationshipEitherWay(stepparent, $$.parent),
    parentOf(kid, $$.parent),
    L.not(parentOf(kid, stepparent)),
  );
});

export const stepParentAgg = L.Rel((k, p) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, stepParentOf(k, in_s), p);
});

export const tap = (msg: any) => {
  return function* (s: any) {
    console.log(msg, s);
    yield s;
  };
};

export const fullSiblingOf = L.Rel((out_v, out_s) => {
  const p1 = L.lvar("fullsibof_p1");
  const p2 = L.lvar("fullsibof_p2");
  return distincto_G(
    out_s,
    L.and(
      parentOf(out_v, p1),
      parentOf(out_v, p2),

      parentOf(out_s, p1),
      parentOf(out_s, p2),

      L.not(L.eq(p1, p2)),
      L.not(L.eq(out_v, out_s)),
    ),
  );
});

export const fullSiblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, fullSiblingOf(v, in_s), s);
});

export const halfSiblingOf = L.Rel((out_v, out_s) => {
  const nonsharedparent_v = L.lvar("halfsibof_nonsharedparent_v");
  const sharedparent = L.lvar("halfsibof_sharedparent");
  const nonsharedparent_s = L.lvar("halfsibof_nonsharedparent_s");
  return L.and(
    parentOf(out_v, sharedparent),
    parentOf(out_s, sharedparent),

    parentOf(out_v, nonsharedparent_v),
    L.not(L.eq(nonsharedparent_v, sharedparent)),

    parentOf(out_s, nonsharedparent_s),
    L.not(L.eq(nonsharedparent_s, sharedparent)),

    L.not(L.eq(nonsharedparent_v, nonsharedparent_s)),

    L.not(L.eq(out_v, out_s)),
  );
});

export const halfSiblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, halfSiblingOf(v, in_s), s);
});

export const stepSiblingOf = L.Rel((out_v: any, out_s: any) => {
  const $$ = L.createLogicVarProxy(undefined, "stepsiblingof_");
  return L.and(
    parentOf(out_v, $$.vparent),
    relationshipEitherWay($$.vparent, $$.Mstepsibling_parent),
    parentOf(out_s, $$.Mstepsibling_parent),
    neq_C(out_v, out_s),
    // tap("SSSS"),
    L.not(parentOf(out_v, $$.Mstepsibling_parent)),
    L.not(parentOf(out_s, $$.vparent)),
  );
});

export const stepSiblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, stepSiblingOf(v, in_s), s);
});

export const siblingOf = L.Rel((v, s) =>
  L.or(fullSiblingOf(v, s), halfSiblingOf(v, s), stepSiblingOf(v, s)),
);

export const siblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, siblingOf(v, in_s), s);
});

// Refactored using uncleOfLevel
export const uncleOf = uncleOfLevel(1);
export const greatuncleOf = uncleOfLevel(2);

export const uncleAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, uncleOf(v, in_s), s);
});

export const greatuncleAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, greatuncleOf(v, in_s), s);
});

// Generalized ancestor relation: ancestorOf(level)(descendant, ancestor)
export function ancestorOf(level: number) {
  return L.Rel((descendant, ancestor) => {
    if (level < 1) return L.eq(descendant, ancestor);
    const chain = [descendant];
    for (let i = 0; i < level; ++i) {
      chain.push(L.lvar(`ancestor_${i}`));
    }
    const goals = [];
    for (let i = 0; i < level; ++i) {
      goals.push(anyParentOf(chain[i], chain[i + 1]));
    }
    goals.push(L.eq(chain[level], ancestor));
    return L.and(...goals);
  });
}

// Generalized uncle/aunt relation: uncleOfLevel(level)(person, uncle)
export function uncleOfLevel(level = 1) {
  return L.Rel((person: any, uncle: any) => {
    const ancestor = L.lvar("uncle_ancestor");
    const sibling = L.lvar("uncle_sibling");
    return L.distincto_G(
      uncle,
      L.and(
        ancestorOf(level)(person, ancestor),
        siblingOf(ancestor, sibling),
        // uncle can be sibling or sibling-in-law
        L.or(L.eq(uncle, sibling), relationshipEitherWay(sibling, uncle)),
        // Exclude direct ancestors
        L.not(anyParentOf(person, uncle)),
      ),
    );
  });
}

// Exclude if candidate is a sibling of any ancestor of person up to maxLevel
function isSiblingOfAnyAncestor(person: any, candidate: any, maxLevel: number) {
  const goals = [];
  for (let i = 1; i < maxLevel; ++i) {
    const anc = L.lvar(`anc_${i}`);
    goals.push(L.and(ancestorOf(i)(person, anc), siblingOf(anc, candidate)));
  }
  return L.or(...goals);
}

// Strict cousin relationship: ensure closest common ancestor is at the correct level
function cousinOf(a: any, b: any, degree = 1, removal = 0): any {
  if (degree < 1) return L.eq(0, 1);
  let levelA, levelB;
  if (removal < 0) {
    levelA = degree + 1 - removal;
    levelB = degree + 1;
  } else {
    levelA = degree + 1;
    levelB = degree + 1 + removal;
  }
  const ancestorA = L.lvar("cousinGen_ancestorA");
  const ancestorB = L.lvar("cousinGen_ancestorB");
  // Constraints to ensure no closer common ancestor
  const noCloserCommon = [];
  for (let i = 1; i < levelA; ++i) {
    const closerA = L.lvar(`closerA_${i}`);
    for (let j = 1; j < levelB; ++j) {
      const closerB = L.lvar(`closerB_${j}`);
      noCloserCommon.push(
        L.not(
          L.and(
            ancestorOf(i)(a, closerA),
            ancestorOf(j)(b, closerB),
            L.eq(closerA, closerB),
          ),
        ),
      );
    }
  }
  return L.distincto_G(
    b,
    L.and(
      ancestorOf(levelA)(a, ancestorA),
      ancestorOf(levelB)(b, ancestorB),
      L.eq(ancestorA, ancestorB),
      ...noCloserCommon,
      // L.not(L.eq(a, b)),
      // L.not(siblingOf(a, b)),
      // L.not(anyParentOf(a, b)),
      // L.not(anyParentOf(b, a)),
      L.not(isSiblingOfAnyAncestor(a, b, Math.max(levelA, levelB))),
      L.not(isSiblingOfAnyAncestor(b, a, Math.max(levelA, levelB))),
    ),
  );
}

export const cousinsAgg = L.Rel((v, s, degree = 1, removal = 0) => {
  const in_s = L.lvar("in_s");
  return L.collecto(in_s, cousinOf(v, in_s, degree, removal), s);
});

export const firstcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 1);
export const secondcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 2);
export const thirdcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 3);

// Nephew/Niece relation: nephewOf(person, nephew)
export const nephewOf = L.Rel((person, nephew) => {
  const parent = L.lvar("nephew_parent");
  return L.distincto_G(
    nephew,
    L.and(
      anyParentOf(nephew, parent),
      siblingOf(person, parent),
      L.not(L.eq(person, nephew)),
    ),
  );
});
