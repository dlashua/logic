import { Term, Goal } from "../core/types.ts";
import { lvar } from "../core/kernel.ts";
import { and, eq, or } from "../core/combinators.ts";
import { createLogicVarProxy } from "../query.ts";
import { uniqueo, not } from "../relations/control.ts";
import { collecto } from "../relations/aggregates.ts";
// import { getCousinsOf } from "../test/direct-sql.ts";

export let parent_kid: (p: Term<string>, k: Term<string>) => Goal;
export let relationship: (a: Term<string|number>, b: Term<string|number>) => Goal;

export function set_parent_kid(fn: (p: Term<string>, k: Term<string>) => Goal) {
  parent_kid = fn;
}

export function set_relationship(fn: (a: Term<string|number>, b: Term<string|number>) => Goal) {
  relationship = fn;
}

export const parentOf = (v: Term<string>, p: Term<string>): Goal => parent_kid(p, v);

export const person = (p: Term<string>): Goal => {
  const { proxy: $ } = createLogicVarProxy("person_");
  return uniqueo(
    p,
    or(parent_kid(p, $.kid), parent_kid($.parent, p)),
  );
};

export const kidsAgg = (v: Term<string>, s: Term<string[]>): Goal => {
  const in_s = lvar("in_s");
  return collecto(in_s, anyKidOf(v, in_s), s);
};

// Refactored using generalized ancestorOf
export const grandparent_kid = (gp: Term<string>, k: Term<string>) => ancestorOf(2)(k,gp);
export const greatgrandparent_kid = (ggp: Term<string>, k: Term<string>) => ancestorOf(3)(k, ggp);

export const grandparentAgg = (k: Term<string>, gp: Term<string[]>): Goal => {
  const { proxy: $ } = createLogicVarProxy("grandparentagg_");
  return collecto($.in_gp, grandparent_kid($.in_gp, k), gp);
};

export const greatgrandparentAgg = (k: Term<string>, gp: Term<string[]>): Goal => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatgrandparent_kid(in_s, k), gp);
};

export const anyParentOf = (v: Term<string>, p: Term<string>): Goal => {
  return or(stepParentOf(v, p), parentOf(v, p));
};

export const anyKidOf = (p: Term<string>, v: Term<string>): Goal => {
  return or(stepKidOf(p, v), parentOf(v, p));
};

export const kidOf = (p: Term<string>, v: Term<string>): Goal => {
  return parentOf(v, p);
};

export const parentAgg = (k: Term<string>, p: Term<string[]>): Goal => {
  const in_s = lvar("in_s");
  return collecto(in_s, parentOf(k, in_s), p);
};

export const stepParentOf = (kid: Term<string>, stepparent: Term<string>) => {
  const { proxy: $ } = createLogicVarProxy("stepparentof_");
  return and(
    parentOf(kid, $.parent),
    relationship($.parent, stepparent),
    not(parentOf(kid, stepparent)),
  );
};

export const stepKidOf = (stepparent: Term<string>, kid: Term<string>) => {
  const { proxy: $ } = createLogicVarProxy("stepkidof_");
  return and(
    relationship(stepparent, $.parent),
    parentOf(kid, $.parent),
    not(parentOf(kid, stepparent)),
  );
};

export const stepParentAgg = (k: Term<string>, p: Term<string[]>) => {
  const in_s = lvar("stepParentAgg_in_s");
  return collecto(in_s, stepParentOf(k, in_s), p);
};

export const tap = (msg: any) => {
  return function* (s: any) {
    console.log(msg, s);
    yield s;
  };
};

export const fullSiblingOf = (out_v: Term<string>, out_s: Term<string>) => {
  const p1 = lvar("fullsibof_p1");
  const p2 = lvar("fullsibof_p2");
  return uniqueo(
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
};

export const fullSiblingsAgg = (v: Term<string>, s: Term<string[]>) => {
  const in_s = lvar("in_s");
  return collecto(in_s, fullSiblingOf(v, in_s), s);
};

export const halfSiblingOf = (out_v: Term<string>, out_s: Term<string>) => {
  const sharedparent = lvar("halfsibof_sharedparent");
  return uniqueo(
    out_s,
    and(
      parentOf(out_v, sharedparent),
      parentOf(out_s, sharedparent),
      not(eq(out_v, out_s)),
      not(fullSiblingOf(out_v, out_s)),
    )
  )
};

export const halfSiblingsAgg = (v: Term<string>, s: Term<string[]>) => {
  const in_s = lvar("in_s");
  return collecto(in_s, halfSiblingOf(v, in_s), s);
};

export const stepSiblingOf = (out_v: Term<string>, out_s: Term<string>) => {
  const { proxy: $ } = createLogicVarProxy("stepsiblingof_");
  return uniqueo(
    out_s,
    and(
      anyParentOf(out_v, $.parent),
      anyKidOf($.parent, out_s),
      not(eq(out_v, out_s)),
      not(halfSiblingOf(out_v, out_s)),
      not(fullSiblingOf(out_v, out_s)),
    )
  )
};

export const stepSiblingsAgg = (v: Term<string>, s: Term<string[]>) => {
  const in_s = lvar("in_s");
  return collecto(in_s, stepSiblingOf(v, in_s), s);
};

export const siblingOf = (v: Term<string>, s: Term<string>) => {
  const in_s = lvar("in_s");
  return uniqueo(s, and(anyParentOf(v, in_s), anyKidOf(in_s, s), not(eq(v, s))))
};

export const siblingsAgg = (v: Term<string>, s: Term<string[]>) => {
  const in_s = lvar("in_s");
  return collecto(in_s, siblingOf(v, in_s), s);
};

// Refactored using uncleOfLevel
export const uncleOf = uncleOfLevel(1);
export const greatuncleOf = uncleOfLevel(2);

export const uncleAgg = (v: Term<string>, s: Term<string[]>, level = 1) => {
  const in_s = lvar("in_s");
  return collecto(in_s, uncleOfLevel(level)(v, in_s), s);
};

export const greatuncleAgg = (v: Term<string>, s: Term<string[]>) => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatuncleOf(v, in_s), s);
};

// Generalized ancestor relation: ancestorOf(level)(descendant, ancestor)
export const ancestorOf = function ancestorOf(level: number) {
  return (descendant: Term<string>, ancestor: Term<string>) => {
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
  };
};


// Generalized uncle/aunt relation: uncleOfLevel(level)(person, uncle)
export function uncleOfLevel(level = 1) {
  return (person: Term<string>, uncle: Term<string>) => {
    const ancestor = lvar("uncle_ancestor");
    const sibling = lvar("uncle_sibling");
    return uniqueo(
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
  };
}

// Classic cousinOf: climb up degree steps from a to ancestor, then down degree-removal steps to b
export function cousinOf(a: Term<string>, b: Term<string>, degree = 1, removal = 0): Goal {
  if (degree < 1) return eq(0,1); // invalid
  const upR = removal > 0 ? removal : 0;
  const downR = removal < 0 ? (removal * -1) : 0;
  const stepsUp = degree + 1 + upR;
  const stepsDown = degree + 1 + downR;
  if (stepsDown < 1) return eq(0, 1); // invalid

  const exclusions = [];
  const commonAncestor = lvar("cousinOf_commonAncestor") as Term<string>;

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
  
  return uniqueo(
    b,
    and(
      ...aUpGoals,
      ...bDownGoals,
      ...exclusions,
    ),
  );
}



// This is cousinOf via direct-sql
// export function xxcousinOf(a: any, b: any, degree = 1, removal = 0): any {
//   const goal = async function* cousinOf (s: Subst) {
//     const a_w = await walk(a, s);
//     const degree_w = await walk(degree, s);
//     const removal_w = await walk(removal, s);
//     const b_w = await walk(b, s);
//     if(isVar(a_w)) return;
//     if(isVar(degree_w)) return;
//     if(isVar(removal_w)) return;
//     const cousins = await getCousinsOf(a_w, degree_w, removal_w);
//     if(!isVar(b_w)) {
//       if(cousins.includes(b_w)) yield s;
//       return;
//     }
//     for(const cousin of cousins) {
//       yield extendSubst(b, cousin, s);
//     }
//     return;
//   };
//   return goal;
// }

export const cousinsAgg = (v: Term<string>, s: Term<string[]>, degree = 1, removal = 0) => {
  const in_s = lvar("in_s");
  return collecto(in_s, cousinOf(v, in_s, degree, removal), s);
};

export const firstcousinsAgg = (v: Term<string>, s: Term<string[]>) => cousinsAgg(v, s, 1);
export const secondcousinsAgg = (v: Term<string>, s: Term<string[]>) => cousinsAgg(v, s, 2);
export const thirdcousinsAgg = (v: Term<string>, s: Term<string[]>) => cousinsAgg(v, s, 3);

// Nephew/Niece relation: nephewOf(person, nephew)
export const nephewOf = (person: Term<string>, nephew: Term<string>) => {
  const parent = lvar("nephew_parent");
  return uniqueo(
    nephew,
    and(
      anyParentOf(nephew, parent),
      siblingOf(person, parent),
      not(eq(person, nephew)),
    ),
  );
};
