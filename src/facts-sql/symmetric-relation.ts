import {
  Term,
  Subst,
  walk,
  unify,
  isVar
} from "../core.ts"
import { Logger } from "../shared/logger.ts";
import { QueryCache } from "./cache.ts";
import { SymmetricPatternManager } from "./pattern-manager.ts";
import { QueryBuilder } from "./query-builder.ts";
import { SymmetricPattern, GoalFunction } from "./types.ts";

export class SymmetricRelation {
  constructor(
    private table: string,
    private keys: [string, string],
    private patternManager: SymmetricPatternManager,
    private logger: Logger,
    private cache: QueryCache,
    private queryBuilder: QueryBuilder,
    private queries: string[],
    private realQueries: string[],
  ) {}

  createGoal(queryObj: Record<string, Term<string | number>>): GoalFunction {
    const goalId = this.patternManager.generateGoalId();
    
    // Create and add pattern
    const pattern = this.patternManager.createPattern(this.table, queryObj, goalId);
    this.patternManager.addPattern(pattern);

    return async function* factsSqlSym(this: SymmetricRelation, s: Subst) {
      const patterns = this.patternManager.getPatternsForGoal(goalId);
      if (patterns.length === 0) {
        return;
      }

      for (const pattern of patterns) {
        const s2 = s;
        for await (const result of this.runPattern(s2, queryObj, pattern)) {
          if (result !== null) {
            yield result;
          }
        }
      }

      this.patternManager.logFinalDiagnostics(goalId);
    }.bind(this);
  }

  private async* runPattern(
    s: Subst,
    queryObj: Record<string, Term<string | number>>,
    pattern: SymmetricPattern
  ): AsyncGenerator<Subst, void, unknown> {
    if (pattern.ran && pattern.rows.length === 0) {
      return;
    }

    const values = Object.values(queryObj);
    if (values.length > 2) return;

    const walkedValues: Term[] = await Promise.all(values.map(x => walk(x, s)));
    if (walkedValues[0] === walkedValues[1]) return;

    this.logger.log("RUN_START", "Starting symmetric relation", {
      pattern,
      queryObj,
      walkedValues 
    });

    const { rows, cacheInfo } = await this.getPatternRows(pattern, walkedValues);
    this.patternManager.updatePatternRows(pattern, rows);

    // Process rows and yield unified substitutions
    for (const row of pattern.rows) {
      // Try first orientation
      const s2 = new Map(s);
      const unified1 = await unify(walkedValues[0], row[this.keys[0]], s2);
      if (unified1) {
        const unified2 = await unify(walkedValues[1], row[this.keys[1]], unified1);
        if (unified2) {
          yield unified2;
          continue;
        }
      }

      // Try second orientation (symmetric)
      const s3 = new Map(s);
      const unified3 = await unify(walkedValues[1], row[this.keys[0]], s3);
      if (unified3) {
        const unified4 = await unify(walkedValues[0], row[this.keys[1]], unified3);
        if (unified4) {
          yield unified4;
        }
      }
    }

    this.logger.log("RUN_END", "Completed symmetric relation", {
      pattern 
    });
  }

  private async getPatternRows(
    pattern: SymmetricPattern,
    walkedValues: Term[]
  ): Promise<{ rows: any[], cacheInfo: any }> {
    // Try pattern cache first
    if (pattern.ran) {
      this.logger.log("PATTERN_CACHE_HIT", "Pattern cache hit", {
        pattern,
        rows: pattern.rows 
      });
      return {
        rows: pattern.rows,
        cacheInfo: {
          type: 'pattern' 
        }
      };
    }

    // Check for matching patterns
    const matchingPattern = this.cache.findMatchingSymmetricPattern(
      this.patternManager.getAllPatterns(),
      pattern
    );
    
    if (matchingPattern) {
      const rows = this.cache.processCachedPatternResult(matchingPattern);
      this.logger.log("PATTERN_CACHE_HIT", "Matching pattern found", {
        pattern,
        matchingPattern,
        rows,
      });
      return {
        rows,
        cacheInfo: {
          type: 'pattern',
          matchingGoals: matchingPattern.goalIds 
        }
      };
    }

    // Execute database query
    return await this.executeQuery(pattern, walkedValues);
  }

  private async executeQuery(
    pattern: SymmetricPattern,
    walkedValues: Term[]
  ): Promise<{ rows: any[], cacheInfo: any }> {
    const groundedValues = walkedValues.filter(x => !isVar(x)) as (string | number)[];
    
    const query = this.queryBuilder.buildSymmetricQuery(
      this.table,
      this.keys,
      groundedValues
    );

    const { rows, sql } = await this.queryBuilder.executeQuery(query);
    (pattern as any).ran = true;

    // Log query
    this.logger.logQuery(sql, rows);
    this.queries.push(sql);
    this.realQueries.push(sql);
    (pattern as any).queries.push(sql);

    return {
      rows,
      cacheInfo: {
        type: 'database',
        sql 
      }
    };
  }
}