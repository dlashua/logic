import type { Term, Var } from "../core/types.ts";
import { isVar } from "../core/kernel.ts";
import { Logger } from "../shared/logger.ts";
import type { QueryPattern } from "./query-merger.ts";

export class PatternProcessor {
  constructor(private logger: Logger) {}

  public findMergeGroups(patterns: QueryPattern[]): QueryPattern[][] {
    const groups: QueryPattern[][] = [];
    const processed = new Set<number>();
    
    this.logger.log("MERGE_DETECTION_START", `Finding merge groups for ${patterns.length} patterns`, {
      patterns: patterns.map(p => ({
        goalId: p.goalId,
        table: p.table,
        varIds: Array.from(p.varIds) 
      }))
    });
    
    for (const pattern of patterns) {
      if (processed.has(pattern.goalId)) {
        continue;
      }
      
      const group = [pattern];
      processed.add(pattern.goalId);
      
      let foundMatch = true;
      while (foundMatch) {
        foundMatch = false;
        const groupVarIds = new Set<string>();
        for (const p of group) {
          for (const varId of p.varIds) {
            groupVarIds.add(varId);
          }
        }
        
        this.logger.log("CHECKING_MERGE_CANDIDATES", `Group ${group.map(p => p.goalId).join(',')} has varIds: ${Array.from(groupVarIds).join(',')}`, {
          groupGoalIds: group.map(p => p.goalId),
          groupVarIds: Array.from(groupVarIds)
        });
        
        for (const otherPattern of patterns) {
          if (processed.has(otherPattern.goalId)) {
            continue;
          }
          
          const sharedVars = Array.from(otherPattern.varIds)
            .filter(varId => groupVarIds.has(varId));
          const hasSharedVar = sharedVars.length > 0;
          
          this.logger.log("MERGE_CANDIDATE_CHECK", `Goal ${otherPattern.goalId} checked against group`, {
            candidateGoalId: otherPattern.goalId,
            candidateVarIds: Array.from(otherPattern.varIds),
            sharedVars,
            hasSharedVar
          });
          
          if (hasSharedVar) {
            group.push(otherPattern);
            processed.add(otherPattern.goalId);
            foundMatch = true;
            
            this.logger.log("MERGE_CANDIDATE_ADDED", `Goal ${otherPattern.goalId} added to group`, {
              goalId: otherPattern.goalId,
              groupGoalIds: group.map(p => p.goalId),
              sharedVars
            });
          }
        }
      }
      
      groups.push(group);
    }
    
    this.logger.log("MERGE_GROUPS_FOUND", `Found ${groups.length} merge groups`, {
      groups: groups.map(g => ({
        goalIds: g.map(p => p.goalId),
        tables: g.map(p => p.table),
        size: g.length
      }))
    });
    
    return groups;
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
    
    this.logger.log("VAR_TO_COLUMNS_MAP", `All variables mapped to columns`, {
      varToColumns: Object.fromEntries(Array.from(varToColumns.entries()).map(([varId, cols]) => [
        varId, 
        cols.map(c => `${c.table}.${c.column}(${c.type})`)
      ]))
    });
    
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
    
    this.logger.log("JOIN_VARIABLES_FOUND", `Found ${joinVars.length} join variables`, {
      joinVars: joinVars.map(jv => ({
        varId: jv.varId,
        columns: jv.columns.map(c => ({
          table: c.table,
          column: c.column,
          goalId: c.goalId,
          type: c.type 
        }))
      }))
    });
    
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