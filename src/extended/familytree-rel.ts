import {
  Term,
  lvar,
  createLogicVarProxy ,
  Goal,
  lift,
  and,
  uniqueo,
  eq,
  not,
  or,
} from "../core.ts"
import { collecto } from "../relations-agg.ts";
// import { getCousinsOf } from "../test/direct-sql.ts";

let parent_kid = (p: Term, k: Term): Goal => {
  throw "must set parent_kid";
};
let relationship = (a: Term<string|number>, b: Term<string|number>): Goal => {
  throw "must set relationship";
};

export function set_parent_kid(fn: typeof parent_kid) {
  parent_kid = lift(fn);
}

export function set_relationship(fn: typeof relationship) {
  relationship = lift(fn);
}

export const parentOf = lift((v, p) => parent_kid(p, v));

export const person = lift((p) => {
  const { proxy: $$ } = createLogicVarProxy("person_");
  return uniqueo(
    p,
    or(parent_kid(p, $$.kid), parent_kid($$.parent, p)),
  );
});

export const kidsAgg = lift((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, anyKidOf(v, in_s), s);
});

// Refactored using generalized ancestorOf
export const grandparent_kid = (gp: Term, k: Term) => ancestorOf(2)(k,gp);
export const greatgrandparent_kid = (ggp: Term, k: Term) => ancestorOf(3)(k, ggp);

export const grandparentAgg = lift((k, gp) => {
  const { proxy: $$ } = createLogicVarProxy("grandparentagg_");
  return collecto($$.in_gp, grandparent_kid($$.in_gp, k), gp);
});

export const greatgrandparentAgg = lift((k, gp) => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatgrandparent_kid(in_s, k), gp);
});

export const anyParentOf = lift((v, p) => {
  return or(stepParentOf(v, p), parentOf(v, p));
});

export const anyKidOf = lift((p, v) => {
  return or(stepKidOf(p, v), parentOf(v, p));
  // return or(parentOf(v, p), parentOf(v, p));

});

export const kidOf = lift((p, v) => {
  return parentOf(v, p);
});

export const parentAgg = lift(function parentAgg (k, p) {
  const in_s = lvar("parentAgg_in_s");
  return collecto(in_s, parentOf(k, in_s), p);
});

export const stepParentOf = lift((kid: any, stepparent: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepparentof_");
  return and(
    parentOf(kid, $$.parent),
    relationship($$.parent, stepparent),
    not(parentOf(kid, stepparent)),
  );
});

export const stepKidOf = lift((stepparent: any, kid: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepkidof_");
  return and(
    relationship(stepparent, $$.parent),
    parentOf(kid, $$.parent),
    not(parentOf(kid, stepparent)),
  );
});

export const stepParentAgg = lift((k, p) => {
  const in_s = lvar("stepParentAgg_in_s");
  return collecto(in_s, stepParentOf(k, in_s), p);
});

export const tap = (msg: any) => {
  return function* (s: any) {
    console.log(msg, s);
    yield s;
  };
};

export const fullSiblingOf = lift((out_v, out_s) => {
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
});

export const fullSiblingsAgg = lift((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, fullSiblingOf(v, in_s), s);
});

export const halfSiblingOf = lift((out_v, out_s) => {
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
});

export const halfSiblingsAgg = lift((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, halfSiblingOf(v, in_s), s);
});

export const stepSiblingOf = lift((out_v: any, out_s: any) => {
  const { proxy: $$ } = createLogicVarProxy("stepsiblingof_");
  return uniqueo(
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

export const stepSiblingsAgg = lift((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, stepSiblingOf(v, in_s), s);
});

export const siblingOf = lift((v, s) => {
  const in_s = lvar("in_s");
  return uniqueo(s, and(anyParentOf(v, in_s), anyKidOf(in_s, s), not(eq(v, s))))
});

export const siblingsAgg = lift((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, siblingOf(v, in_s), s);
});

// Refactored using uncleOfLevel
export const uncleOf = uncleOfLevel(1);
export const greatuncleOf = uncleOfLevel(2);

export const uncleAgg = lift((v, s, level = 1) => {
  const in_s = lvar("in_s");
  return collecto(in_s, uncleOfLevel(level)(v, in_s), s);
});

export const greatuncleAgg = lift((v, s) => {
  const in_s = lvar("in_s");
  return collecto(in_s, greatuncleOf(v, in_s), s);
});

// Generalized ancestor relation: ancestorOf(level)(descendant, ancestor)
export const ancestorOf = function ancestorOf(level: number) {
  return lift((descendant, ancestor) => {
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
  return lift((person: any, uncle: any) => {
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

export const cousinsAgg = lift((v, s, degree = 1, removal = 0) => {
  const in_s = lvar("in_s");
  return collecto(in_s, cousinOf(v, in_s, degree, removal), s);
});

export const firstcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 1);
export const secondcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 2);
export const thirdcousinsAgg = (v: any, s: any) => cousinsAgg(v, s, 3);

// Nephew/Niece relation: nephewOf(person, nephew)
export const nephewOf = lift((person, nephew) => {
  const parent = lvar("nephew_parent");
  return uniqueo(
    nephew,
    and(
      anyParentOf(nephew, parent),
      siblingOf(person, parent),
      not(eq(person, nephew)),
    ),
  );
});