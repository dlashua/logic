import type { Term, Subst, Goal } from "../core/types.ts";
import { unify, isVar, walk } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import { RelationOptions } from "./types.ts";
import type { DBManager } from "./index.ts";

export class RegularRelationWithMerger {
  
  constructor(
    private dbObj: DBManager,
    private table: string,
    private logger: Logger,
    options?: RelationOptions,
  ) {
  }

  createGoal(queryObj: Record<string, Term>): Goal {
    const baseGoalId = this.dbObj.getNextGoalId();
    

    return async function* factsSql(this: RegularRelationWithMerger, s: Subst) {
      const walkedQuery: Record<string, Term> = {};
      const selectCols: Record<string, Term> = {};
      const whereCols: Record<string, Term> = {};
      
      for (const [column, term] of Object.entries(queryObj)) {
        const walkedTerm = await walk(term, s);
        walkedQuery[column] = walkedTerm;
        
        if (isVar(walkedTerm)) {
          selectCols[column] = walkedTerm;
        } else {
          whereCols[column] = walkedTerm;
        }
      }

      const selectColKeys = Object.keys(queryObj)
      const query = this.dbObj.db(this.table).select(selectColKeys).where(whereCols);
      this.dbObj.addQuery(query.toString());
      const rows = await query;
      
      let yielded = 0;
      for (const row of rows) {

        const unifiedSubst = await this.unifyRowWithQuery(row, queryObj, s);
        if (unifiedSubst) {
          yielded++;

          yield unifiedSubst;
        }
      }
    }.bind(this);
  }

  private async unifyRowWithQuery(
    row: any,
    queryObj: Record<string, Term>,
    s: Subst
  ): Promise<Subst | null> {
    // Optimized unification - directly match variable IDs with row values
    let resultSubst = s;
    
    for (const [col, term] of Object.entries(queryObj)) {
      const rowValue = row[col];
      if (rowValue === undefined) {
        return null; // Variable not found in result set
      }

      const unified = await unify(term, rowValue, resultSubst);
      if (!unified) {
        return null; // Unification failed
      }
      resultSubst = unified;

    }
    
    return resultSubst;
  }

}