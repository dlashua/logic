import * as L from "./logic_lib.ts";
import { neq_C, distincto_G, distincto_C } from "./relations.ts";

let parent_kid = () => { throw "must set parent_kid"};
let relationship = () => { throw "must set relationship"};

export function set_parent_kid(fn) {
  parent_kid = fn;
}

export function set_relationship (fn) {
  relationship = fn;
}

export const parentOf = L.Rel((v, p) => parent_kid(p, v));

export const person = L.Rel((p) => {
  const $$ = L.createLogicVarProxy(undefined, "person_");
  return L.and(
    L.or(
      parent_kid(p, $$.kid),
      parent_kid($$.parent, p),
    ),
    distincto_C(p),
  );
});

// export const kidsAgg = L.groupAggregateRelFactory((v, k) => L.distinctVar(
//   k,
//   parent_kid(v, k),
// ),
// );

export const kidsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    anyKidOf(v, in_s),
    s,
  );
});

export const grandparent_kid = L.Rel((gp, k) => {
  const $$ = L.createLogicVarProxy(undefined, "grandparent_kid_");
  return L.and(
    anyParentOf(k, $$.p),
    anyParentOf($$.p, gp),
  )
});

export const greatgrandparent_kid = L.Rel((ggp, k) => {
  const $$ = L.createLogicVarProxy(undefined, "greatgrandparent_kid_");
  return L.and(
    anyParentOf(k, $$.p),
    grandparent_kid(ggp, $$.p),
  )
});

export const grandparentAgg = L.Rel((k, gp) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    grandparent_kid(in_s, k),
    gp,
  );
});

export const greatgrandparentAgg = L.Rel((k, gp) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    greatgrandparent_kid(in_s, k),
    gp,
  );
});

export const anyParentOf = L.Rel((v, p) => {
  const pp = L.lvar("anyparentof_parent");
  const sp = L.lvar("anyparentof_stepparent");
  return L.and(
    L.or(
      L.and(
        stepParentOf(v, p),
      ),
      L.and(
        parentOf(v, p),
      ),
    ),
  );
});

export const anyKidOf = L.Rel((p, v) => {
  const pp = L.lvar("anyparentof_parent");
  const sp = L.lvar("anyparentof_stepparent");
  return L.and(
    L.or(
      L.and(
        stepKidOf(p, v),
      ),
      L.and(
        parentOf(v, p),
      ),
    ),
  );
});

export const parentAgg = L.Rel((k, p) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    parentOf(k, in_s),
    p,
  );
});

export const stepParentOf = L.Rel((kid, stepparent) => {
  const $$ = L.createLogicVarProxy(undefined, "stepparentof_");
  return L.and(
    parentOf(kid, $$.parent),
    L.or(
      L.and(
        relationship($$.parent, stepparent),
        neq_C(stepparent, $$.parent),
        L.not(parentOf(kid, stepparent)),
      ),
      L.and(
        relationship(stepparent, $$.parent),
        neq_C(stepparent, $$.parent),
        L.not(parentOf(kid, stepparent)),
      ),
    ),
  );
});

export const stepKidOf = L.Rel((stepparent, kid) => {
  const $$ = L.createLogicVarProxy(undefined, "stepkidof_");
  return L.and(
    L.or(
      L.and(
        relationship($$.parent, stepparent),
        neq_C(stepparent, $$.parent),
        parentOf(kid, $$.parent),
        L.not(parentOf(kid, stepparent)),
      ),
      L.and(
        relationship(stepparent, $$.parent),
        neq_C(stepparent, $$.parent),
        parentOf(kid, $$.parent),
        L.not(parentOf(kid, stepparent)),
      ),
    ),
  );
});

export const stepParentAgg = L.Rel((k, p) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    stepParentOf(k, in_s),
    p,
  );
});

export const tap = (msg) => {
  return function* (s) {
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
  return L.collecto(
    in_s,
    fullSiblingOf(v, in_s),
    s,
  );
});

export const halfSiblingOf = L.Rel((out_v, out_s) => {
  const nonsharedparent_v = L.lvar("halfsibof_nonsharedparent_v");
  const sharedparent = L.lvar("halfsibof_sharedparent");
  const nonsharedparent_s = L.lvar("halfsibof_nonsharedparent_s");
  return (
    L.and(
      parentOf(out_v, sharedparent),
      parentOf(out_s, sharedparent),
      
      parentOf(out_v, nonsharedparent_v),
      L.not(L.eq(nonsharedparent_v, sharedparent)),

      parentOf(out_s, nonsharedparent_s),
      L.not(L.eq(nonsharedparent_s, sharedparent)),

      L.not(L.eq(nonsharedparent_v, nonsharedparent_s)),
      
      L.not(L.eq(out_v, out_s)),
    )
  );
});

export const halfSiblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    halfSiblingOf(v, in_s),
    s,
  );
});

export const stepSiblingOf = L.Rel((out_v, out_s) => {
  const $$ = L.createLogicVarProxy(undefined, "stepsiblingof_");
  return (
    L.and(
      parentOf(out_v, $$.vparent),
      L.or(
        relationship($$.vparent, $$.Mstepsibling_parent),
        relationship($$.Mstepsibling_parent, $$.vparent),
      ),
      parentOf(out_s, $$.Mstepsibling_parent),
      neq_C(out_v, out_s),
      // tap("SSSS"),
      L.not(parentOf(out_v, $$.Mstepsibling_parent)),
      L.not(parentOf(out_s, $$.vparent)),
    )
  );
});

export const stepSiblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    stepSiblingOf(v, in_s),
    s,
  );
});

export const siblingOf = L.Rel((v, s) => L.or(
  fullSiblingOf(v, s),
  halfSiblingOf(v, s),
  stepSiblingOf(v, s),
),
);

export const siblingsAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    siblingOf(v, in_s),
    s,
  );
});

export const cousinOf = L.Rel((out_v, out_c, level = 1) => {
  // Build ancestor chain for out_v
  const upVars = [out_v];
  for (let i = 0; i <= level; ++i) {
    upVars.push(L.lvar(`cousinOf_up_${i}`));
  }

  // Build descendant chain for out_c (start at highest ancestor and work down)
  const downVars = [upVars[upVars.length - 1]];
  for (let i = level - 1; i >= 0; --i) {
    downVars.push(L.lvar(`cousinOf_down_${i}`));
  }
  downVars.push(out_c);

  // Constraints for going up from out_v
  const goals = [];
  const gt = [];
  for (let i = 0; i <= level; ++i) {
    goals.push(anyParentOf(upVars[i], upVars[i + 1]));
    gt.push(`apo ${upVars[i].id} ${upVars[i + 1].id}`)
  }

  // Constraints for going down to out_c (reverse order)
  for (let i = 0; i <= level; ++i) {
    
    goals.push(anyKidOf(downVars[i], downVars[i + 1]));
    gt.push(`ako ${downVars[i].id} ${downVars[i + 1].id}`)
    
    goals.push(neq_C(downVars[i + 1], upVars[level - i]));
    if(i < level) {
      gt.push(`neq ${downVars[i + 1].id} ${upVars[level - i].id}`)
    }
  }

  // console.log("CCCCC", level, gt);

  return L.and(
    ...goals,
    distincto_C(out_c),
  );
});

export const firstcousinOf = (a, b) => cousinOf(a, b, 1);
export const secondcousinOf = (a, b) => cousinOf(a, b, 2);
export const thirdcousinOf = (a, b) => cousinOf(a, b, 3);

// Generalized cousinsAgg relation
export const cousinsAgg = L.Rel((v, s, level = 1) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    cousinOf(v, in_s, level),
    s,
  );
});

export const firstcousinsAgg = (v, s) => cousinsAgg(v, s, 1);
export const secondcousinsAgg = (v, s) => cousinsAgg(v, s, 2);
export const thirdcousinsAgg = (v, s) => cousinsAgg(v, s, 3);

export const uncleOf = L.Rel((out_v, out_c) => {
  const $$ = L.createLogicVarProxy(undefined, "uncleof_");
  return (
    L.and(
      anyParentOf(out_v, $$.p),
      anyParentOf($$.p, $$.p1),
      anyKidOf($$.p1, $$.u),
      L.or(
        L.eq(out_c, $$.u),
        L.and(
          relationship($$.u, $$.us),
          L.eq(out_c, $$.us),
        ),
        L.and(
          relationship($$.us, $$.u),
          L.eq(out_c, $$.us),
        ),
      ),
      L.not(anyParentOf(out_v, out_c)),
      neq_C(out_c, $$.p),
      distincto_C(out_c),
    )
  );
});

export const uncleAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    uncleOf(v, in_s),
    s,
  );
});

export const greatuncleOf = L.Rel((out_v, out_c) => {
  const $$ = L.createLogicVarProxy(undefined, "uncleof_");
  return (
    L.and(
      L.and(
        anyParentOf(out_v, $$.p),
        anyParentOf($$.p, $$.p1),
        anyParentOf($$.p1, $$.p2),
        anyKidOf($$.p2, $$.u),
        L.not(anyParentOf($$.p, $$.u)),
        L.or(
          L.eq(out_c, $$.u),
          L.and(
            relationship($$.u, $$.us),
            L.eq(out_c, $$.us),
          ),
          L.and(
            relationship($$.us, $$.u),
            L.eq(out_c, $$.us),
          ),
        ),
        L.not(anyParentOf(out_v, out_c)),
        neq_C(out_c, $$.p),
        neq_C(out_c, $$.p1),
        neq_C(out_c, $$.p2),
        distincto_C(out_c),
      ),
    )
  );
});

export const greatuncleAgg = L.Rel((v, s) => {
  const in_s = L.lvar("in_s");
  return L.collecto(
    in_s,
    greatuncleOf(v, in_s),
    s,
  );
});


