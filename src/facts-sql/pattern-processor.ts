import type { Term, Var } from "../core/types.ts";
import { isVar } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import type { QueryPattern } from "./query-merger.ts";

export class PatternProcessor {
  constructor(private logger: Logger) {}

  /**
   * Determines if one query pattern's WHERE clause is a superset of another's.
   * This is true if all of the superset's grounded WHERE conditions are also present in the subset.
   */
  private isSupersetQuery(superset: QueryPattern, subset: QueryPattern): boolean {
    if (superset.table !== subset.table) {
      return false;
    }

    for (const [key, value] of Object.entries(superset.whereCols)) {
      if (isVar(value)) continue;
      if (subset.whereCols[key] !== value) {
        return false;
      }
    }
    return true;
  }

  /**
   * Determines if two query patterns are functionally identical.
   * They're identical if they query the same table with the same WHERE conditions
   * and select the same columns (regardless of variable IDs).
   */
  private areIdenticalQueries(p1: QueryPattern, p2: QueryPattern): boolean {
    if (p1.table !== p2.table) {
      return false;
    }

    // Check if WHERE clauses are identical
    const p1WhereKeys = Object.keys(p1.whereCols).sort();
    const p2WhereKeys = Object.keys(p2.whereCols).sort();
    if (p1WhereKeys.length !== p2WhereKeys.length) {
      return false;
    }
    for (let i = 0; i < p1WhereKeys.length; i++) {
      if (p1WhereKeys[i] !== p2WhereKeys[i]) {
        return false;
      }
      const p1Value = p1.whereCols[p1WhereKeys[i]];
      const p2Value = p2.whereCols[p2WhereKeys[i]];
      if (p1Value !== p2Value) {
        return false;
      }
    }

    // Check if SELECT clauses are identical (same columns, regardless of variable IDs)
    const p1SelectKeys = Object.keys(p1.selectCols).sort();
    const p2SelectKeys = Object.keys(p2.selectCols).sort();
    if (p1SelectKeys.length !== p2SelectKeys.length) {
      return false;
    }
    for (let i = 0; i < p1SelectKeys.length; i++) {
      if (p1SelectKeys[i] !== p2SelectKeys[i]) {
        return false;
      }
      // Both values should be variables (we don't care about their IDs for identity)
      const p1Value = p1.selectCols[p1SelectKeys[i]];
      const p2Value = p2.selectCols[p2SelectKeys[i]];
      if (isVar(p1Value) !== isVar(p2Value)) {
        return false;
      }
      // If both are not variables, they should be equal
      if (!isVar(p1Value) && p1Value !== p2Value) {
        return false;
      }
    }

    return true;
  }

  /**
   * Finds groups of patterns that can be merged into a single SQL query.
   * This is done by treating patterns as nodes in a graph and finding connected components.
   * An edge exists between two patterns if they can be merged.
   */
  public findMergeGroups(patterns: QueryPattern[]): QueryPattern[][] {
    if (patterns.length === 0) {
      return [];
    }

    const adj = new Map<number, number[]>();
    const patternsById = new Map(patterns.map(p => [p.goalId, p]));

    // Initialize adjacency list for all patterns.
    for (const p of patterns) {
      adj.set(p.goalId, []);
    }

    // Build the adjacency list. An edge exists if patterns can be merged.
    for (let i = 0; i < patterns.length; i++) {
      for (let j = i + 1; j < patterns.length; j++) {
        const p1 = patterns[i];
        const p2 = patterns[j];

        // Condition 1: They share at least one logic variable.
        const p1Vars = new Set(p1.varIds);
        const hasSharedVar = Array.from(p2.varIds).some(v => p1Vars.has(v));

        // Condition 2: They are for the same table and one is a superset of the other.
        const isSuperset = (p1.table === p2.table) && 
                               (this.isSupersetQuery(p1, p2) || this.isSupersetQuery(p2, p1));

        // Condition 3: They are functionally identical queries.
        const areIdentical = this.areIdenticalQueries(p1, p2);

        if (hasSharedVar || isSuperset || areIdentical) {
                adj.get(p1.goalId)!.push(p2.goalId);
                adj.get(p2.goalId)!.push(p1.goalId);
        }
      }
    }

    // Find all connected components in the graph using DFS. Each component is a merge group.
    const finalGroups: QueryPattern[][] = [];
    const visited = new Set<number>();
    for (const pattern of patterns) {
      if (visited.has(pattern.goalId)) continue;

      const component: QueryPattern[] = [];
      const stack = [pattern.goalId];
      visited.add(pattern.goalId);

      while (stack.length > 0) {
        const currentId = stack.pop()!;
        component.push(patternsById.get(currentId)!);

        const neighbors = adj.get(currentId) || [];
        for (const neighborId of neighbors) {
          if (!visited.has(neighborId)) {
            visited.add(neighborId);
            stack.push(neighborId);
          }
        }
      }
      finalGroups.push(component);
    }
    
    this.logger.log("MERGE_GROUPS_FOUND", `Found ${finalGroups.length} final merge groups`, {
      groups: finalGroups.map(g => ({
        goalIds: g.map(p => p.goalId),
        tables: Array.from(new Set(g.map(p => p.table))),
        size: g.length
      }))
    });

    return finalGroups;
  }

  public findJoinVariables(patterns: QueryPattern[]): { varId: string; columns: { table: string; column: string; goalId: number; type: 'select' | 'where' }[] }[] {
    const varToColumns = new Map<string, { table: string; column: string; goalId: number; type: 'select' | 'where' }[]>();
    
    for (const pattern of patterns) {
      for (const [column, term] of Object.entries(pattern.selectCols)) {
        if (isVar(term)) {
          const varId = term.id;
          if (!varToColumns.has(varId)) {
            varToColumns.set(varId, []);
          }
          varToColumns.get(varId)!.push({ 
            table: pattern.table, 
            column, 
            goalId: pattern.goalId, 
            type: 'select' 
          });
        }
      }
      
      for (const [column, term] of Object.entries(pattern.whereCols)) {
        if (isVar(term)) {
          const varId = term.id;
          if (!varToColumns.has(varId)) {
            varToColumns.set(varId, []);
          }
          varToColumns.get(varId)!.push({ 
            table: pattern.table, 
            column, 
            goalId: pattern.goalId, 
            type: 'where' 
          });
        }
      }
    }
    
    const joinVars = [];
    for (const [varId, columns] of varToColumns.entries()) {
      const goalIds = new Set(columns.map(c => c.goalId));
      
      if (goalIds.size > 1) {
        joinVars.push({
          varId,
          columns 
        });
      }
    }
    
    return joinVars;
  }

  public separateQueryColumns(queryObj: Record<string, Term>): {
    selectCols: Record<string, Term>;
    whereCols: Record<string, Term>;
  } {
    const selectCols: Record<string, Term> = {};
    const whereCols: Record<string, Term> = {};

    for (const [key, value] of Object.entries(queryObj)) {
      if (isVar(value)) {
        selectCols[key] = value;
      } else {
        whereCols[key] = value;
      }
    }

    return {
      selectCols,
      whereCols 
    };
  }

  public extractVarIds(queryObj: Record<string, Term>): Set<string> {
    const varIds = new Set<string>();
    
    for (const value of Object.values(queryObj)) {
      if (isVar(value)) {
        varIds.add((value as Var).id);
      }
    }
    
    return varIds;
  }

  public buildJoinSelectColumns(patterns: QueryPattern[]): string[] {
    const selectCols = [];
    
    for (const pattern of patterns) {
      for (const column of Object.keys(pattern.selectCols)) {
        selectCols.push(`${pattern.table}.${column} AS ${pattern.table}_${column}`);
      }
    }
    
    return selectCols;
  }

  public buildJoinWhereConditions(patterns: QueryPattern[]): Record<string, any> {
    const whereCols: Record<string, any> = {};
    
    for (const pattern of patterns) {
      for (const [column, value] of Object.entries(pattern.whereCols)) {
        whereCols[`${pattern.table}.${column}`] = value;
      }
    }
    
    return whereCols;
  }
}
