import { Term, Goal } from "../core/types.ts";
import { lvar } from "../core/kernel.ts";
import { and, eq, or } from "../core/combinators.ts";
import { createLogicVarProxy } from "../query.ts";
import { uniqueo, not } from "../relations/control.ts";
import { collecto } from "../relations/aggregates.ts";
// import { getCousinsOf } from "../test/direct-sql.ts";

export class FamilytreeRelations {
  private parent_kid: (p: Term<string>, k: Term<string>) => Goal;
  private relationship: (a: Term<string|number>, b: Term<string|number>) => Goal;

  constructor(
    parent_kid: (p: Term<string>, k: Term<string>) => Goal,
    relationship: (a: Term<string|number>, b: Term<string|number>) => Goal
  ) {
    this.parent_kid = parent_kid;
    this.relationship = relationship;
  }

  parentOf = (v: Term<string>, p: Term<string>): Goal => this.parent_kid(p, v);

  person = (p: Term<string>): Goal => {
    const { proxy: $ } = createLogicVarProxy("person_");
    return uniqueo(
      p,
      or(this.parent_kid(p, $.kid), this.parent_kid($.parent, p)),
    );
  };

  kidsAgg = (v: Term<string>, s: Term<string[]>): Goal => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.anyKidOf(v, in_s), s);
  };

  // Refactored using generalized ancestorOf
  grandparent_kid = (gp: Term<string>, k: Term<string>) => this.ancestorOf(2)(k,gp);
  greatgrandparent_kid = (ggp: Term<string>, k: Term<string>) => this.ancestorOf(3)(k, ggp);

  grandparentAgg = (k: Term<string>, gp: Term<string[]>): Goal => {
    const { proxy: $ } = createLogicVarProxy("grandparentagg_");
    return collecto($.in_gp, this.grandparent_kid($.in_gp, k), gp);
  };

  greatgrandparentAgg = (k: Term<string>, gp: Term<string[]>): Goal => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.greatgrandparent_kid(in_s, k), gp);
  };

  anyParentOf = (v: Term<string>, p: Term<string>): Goal => {
    return or(this.stepParentOf(v, p), this.parentOf(v, p));
  };

  anyKidOf = (p: Term<string>, v: Term<string>): Goal => {
    return or(this.stepKidOf(p, v), this.parentOf(v, p));
  };

  kidOf = (p: Term<string>, v: Term<string>): Goal => {
    return this.parentOf(v, p);
  };

  parentAgg = (k: Term<string>, p: Term<string[]>): Goal => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.parentOf(k, in_s), p);
  };

  stepParentOf = (kid: Term<string>, stepparent: Term<string>) => {
    const { proxy: $ } = createLogicVarProxy("stepparentof_");
    return and(
      this.parentOf(kid, $.parent),
      this.relationship($.parent, stepparent),
      not(this.parentOf(kid, stepparent)),
    );
  };

  stepKidOf = (stepparent: Term<string>, kid: Term<string>) => {
    const { proxy: $ } = createLogicVarProxy("stepkidof_");
    return and(
      this.relationship(stepparent, $.parent),
      this.parentOf(kid, $.parent),
      not(this.parentOf(kid, stepparent)),
    );
  };

  stepParentAgg = (k: Term<string>, p: Term<string[]>) => {
    const in_s = lvar("stepParentAgg_in_s");
    return collecto(in_s, this.stepParentOf(k, in_s), p);
  };

  tap = (msg: any) => {
    return function* (s: any) {
      console.log(msg, s);
      yield s;
    };
  };

  fullSiblingOf = (out_v: Term<string>, out_s: Term<string>) => {
    const p1 = lvar("fullsibof_p1");
    const p2 = lvar("fullsibof_p2");
    return uniqueo(
      out_s,
      and(
        this.parentOf(out_v, p1),
        this.parentOf(out_v, p2),

        this.parentOf(out_s, p1),
        this.parentOf(out_s, p2),

        not(eq(p1, p2)),
        not(eq(out_v, out_s)),
      ),
    );
  };

  fullSiblingsAgg = (v: Term<string>, s: Term<string[]>) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.fullSiblingOf(v, in_s), s);
  };

  halfSiblingOf = (out_v: Term<string>, out_s: Term<string>) => {
    const sharedparent = lvar("halfsibof_sharedparent");
    return uniqueo(
      out_s,
      and(
        this.parentOf(out_v, sharedparent),
        this.parentOf(out_s, sharedparent),
        not(eq(out_v, out_s)),
        not(this.fullSiblingOf(out_v, out_s)),
      )
    )
  };

  halfSiblingsAgg = (v: Term<string>, s: Term<string[]>) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.halfSiblingOf(v, in_s), s);
  };

  stepSiblingOf = (out_v: Term<string>, out_s: Term<string>) => {
    const { proxy: $ } = createLogicVarProxy("stepsiblingof_");
    return uniqueo(
      out_s,
      and(
        this.anyParentOf(out_v, $.parent),
        this.anyKidOf($.parent, out_s),
        not(eq(out_v, out_s)),
        not(this.halfSiblingOf(out_v, out_s)),
        not(this.fullSiblingOf(out_v, out_s)),
      )
    )
  };

  stepSiblingsAgg = (v: Term<string>, s: Term<string[]>) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.stepSiblingOf(v, in_s), s);
  };

  siblingOf = (v: Term<string>, s: Term<string>) => {
    const in_s = lvar("in_s");
    return uniqueo(s, and(this.anyParentOf(v, in_s), this.anyKidOf(in_s, s), not(eq(v, s))))
  };

  siblingsAgg = (v: Term<string>, s: Term<string[]>) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.siblingOf(v, in_s), s);
  };

  // Refactored using uncleOfLevel  
  get uncleOf() { return this.uncleOfLevel(1); }
  get greatuncleOf() { return this.uncleOfLevel(2); }

  uncleAgg = (v: Term<string>, s: Term<string[]>, level = 1) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.uncleOfLevel(level)(v, in_s), s);
  };

  greatuncleAgg = (v: Term<string>, s: Term<string[]>) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.greatuncleOf(v, in_s), s);
  };

  // Generalized ancestor relation: ancestorOf(level)(descendant, ancestor)
  ancestorOf(level: number) {
    return (descendant: Term<string>, ancestor: Term<string>) => {
      if (level < 1) return eq(descendant, ancestor);
      const chain = [descendant];
      for (let i = 0; i < level; ++i) {
        chain.push(lvar(`ancestor_${i}`));
      }
      const goals = [];
      for (let i = 0; i < level; ++i) {
        goals.push(this.anyParentOf(chain[i], chain[i + 1]));
      }
      goals.push(eq(chain[level], ancestor));
      return and(...goals);
    };
  }


  // Generalized uncle/aunt relation: uncleOfLevel(level)(person, uncle)
  uncleOfLevel(level = 1) {
    return (person: Term<string>, uncle: Term<string>) => {
      const ancestor = lvar("uncle_ancestor");
      const sibling = lvar("uncle_sibling");
      return uniqueo(
        uncle,
        and(
          this.ancestorOf(level)(person, ancestor),
          this.siblingOf(ancestor, sibling),
          // uncle can be sibling or sibling-in-law
          or(eq(uncle, sibling), this.relationship(sibling, uncle)),
          // Exclude direct ancestors
          not(this.anyParentOf(person, uncle)),
        ),
      );
    };
  }

  // Classic cousinOf: climb up degree steps from a to ancestor, then down degree-removal steps to b
  cousinOf(a: Term<string>, b: Term<string>, degree = 1, removal = 0): Goal {
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
      aUpGoals.push(this.anyParentOf(prevA, anc));
      prevA = anc;
    }

    let prevB = commonAncestor;
    const bDownGoals = [];
    for (let i = 1; i <= stepsDown; ++i) {
      const kid = (i === stepsDown) ? b : lvar(`cousinOf_b_down_${i}`);
      bDownGoals.push(this.anyKidOf(prevB, kid));
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

  cousinsAgg = (v: Term<string>, s: Term<string[]>, degree = 1, removal = 0) => {
    const in_s = lvar("in_s");
    return collecto(in_s, this.cousinOf(v, in_s, degree, removal), s);
  };

  firstcousinsAgg = (v: Term<string>, s: Term<string[]>) => this.cousinsAgg(v, s, 1);
  secondcousinsAgg = (v: Term<string>, s: Term<string[]>) => this.cousinsAgg(v, s, 2);
  thirdcousinsAgg = (v: Term<string>, s: Term<string[]>) => this.cousinsAgg(v, s, 3);

  // Nephew/Niece relation: nephewOf(person, nephew)
  nephewOf = (person: Term<string>, nephew: Term<string>) => {
    const parent = lvar("nephew_parent");
    return uniqueo(
      nephew,
      and(
        this.anyParentOf(nephew, parent),
        this.siblingOf(person, parent),
        not(eq(person, nephew)),
      ),
    );
  };
}
