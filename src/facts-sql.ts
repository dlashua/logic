import type { Knex } from "knex";
 
import knex from "knex";
import {
  EOSseen,
  EOSsent,
  Subst,
  Term,
  isVar,
  unify,
  walk
} from "./core.ts";

const PATTERN_CACHE_ENABLED = true;
const ROW_CACHE_ENABLED = false;
const RECORD_CACHE_ENABLED = false;
// --- Logging configuration ---
const LOG_ENABLED = true; // Set to true to enable logging globally
const LOG_IDS_TO_IGNORE = new Set<string>([
  "MULTIPLE_ROWS_SELECTCOLS_UNCHANGED",
  "PATTERN_ROWS_UPDATED",
  "MERGING_PATTERNS",
  "GROUNDING_SELECT_COL",
  "RUN_START",
  "PATTERN_CACHE_HIT",
  "ROW_CACHE_HIT",
  "QUERY_CACHE_HIT",
  "DB_QUERY",
  "ROW_CACHE_SET",
  "CACHE_HIT",
  "MATCHED_PATTERNS",
  "UNMATCHED_QUERYOBJ",
  "MERGE_PATTERNS_START",
  "SKIPPED_PATTERN",
  "MERGE_PATTERNS_END",
  "RUN_END",
  "PATTERNS AFTER",
  "RAN FALSE PATTERNS",
  "PATTERNS BEFORE",
  "FINAL PATTERNS",
  "FINAL PATTERNS SYM",

]); // List of log IDs to ignore

const CRITICAL_LOG_IDS = new Set<string>([ "SELECTCOLS MISMATCH PATTERNS"]);

function log(
  id: string,
  ...args: Record<string, unknown>[]
) {
  if (!LOG_ENABLED) return;
  if (LOG_IDS_TO_IGNORE.has(id) && !CRITICAL_LOG_IDS.has(id)) return; // Show logs unless they are ignored and not critical
  if (args.length === 0) {
    console.dir(
      {
        log: id,
      },
      {
        depth: null,
      },
    );
  } else if(args.length === 1) {
    console.dir(
      {
        log: id,
        ...args[0],
      },
      {
        depth: null,
      },
    );
  }else {
    console.dir(
      {
        log: id,
        args,
      },
      {
        depth: null,
      },
    );
  }
}

// Helper: walk all keys of an object with a subst and return a new object
async function walkAllKeys<T extends Record<string, Term>>(
  obj: T,
  subst: Subst
): Promise<Record<string, Term>> {
  const result: Record<string, Term> = {};
  for (const key of Object.keys(obj)) {
    result[key] = await walk(obj[key], subst);
  }
  return result;
}

// Helper: check if all query parameters are grounded (no variables)
function allParamsGrounded(params: Record<string, Term>): boolean {
  for (const key in params) {
    if (isVar(params[key])) return false;
  }
  return true;
}

// Helper: unify all selectCols in a row with walkedQ and subst
async function unifyRowWithWalkedQ(
  selectCols: string[],
  walkedQ: Record<string, Term>,
  row: Record<string, any>,
  subst: Subst,
): Promise<Subst | null> {
  let s2: Subst = new Map(subst);
  for (const col of selectCols) {
    if (!isVar(walkedQ[col])) {
      if (walkedQ[col] === row[col]) {
        continue;
      } else {
        return null;
      }
    } else {
      const unified = await unify(walkedQ[col], row[col], s2);
      if (unified) {
        s2 = unified;
      } else {
        return null;
      }
    }
  }
  return s2;
}

export const makeRelDB = async (
  knex_connect_options: Knex.Config,
  opts?: Record<string, string>,
) => {
  opts ??= {};
  const db = knex(knex_connect_options);
  // Simple cache for queries
  const recordCache = new Map<string, any>();

  // Query logging arrays
  const queries: string[] = [];
  const realQueries: string[] = [];
  const cacheQueries: string[] = [];

  // Helper to build selectCols, whereClauses, walkedQ
  async function buildQueryParts(params: Record<string, Term>, subst: Subst) {
    const selectCols = Object.keys(params).sort();
    const walkedQ: Record<string, Term> = {};
    const whereClauses: { col: string; val: Term }[] = [];
    for (const col of selectCols) {
      walkedQ[col] = await walk(params[col], subst);
      if (!isVar(walkedQ[col])) {
        whereClauses.push({
          col,
          val: walkedQ[col] 
        });
      }
    }
    return {
      selectCols,
      whereClauses,
      walkedQ 
    };
  }

  // Helper: normalize a query key for cache
  function makeCacheKey(table: string, selectCols: string[], whereClauses: { col: string; val: Term }[]) {
    return JSON.stringify({
      table,
      select: [...selectCols].sort(),
      where: [...whereClauses].sort((a, b) => a.col.localeCompare(b.col)),
    });
  }

  // Helper: build a row cache key for fully grounded queries
  function makeRowCacheKey(table: string, params: Record<string, Term>) {
    const key = Object.keys(params).sort().map(k => `${k}:${params[k]}`).join("|");
    return `${table}|${key}`;
  }
  // Row cache for fully grounded queries
  const rowCache = new Map<string, any>();

  // Main relation generator: exact query and row cache, with logging
  const rel = async (table: string) => {
    let nextGoalId = 1;
    interface Pattern { table: string; goalIds: number[]; rows: any[]; ran: boolean; selectCols: Record<string, Term>; whereCols: Record<string, Term>; last: { selectCols: Record<string, Term>[]; whereCols: Record<string, Term>[] }, queries: string[]; selectColsUngrounded?: boolean }
    const patterns: Pattern[] = [];

    return function goal(queryObj: Record<string, Term>) {
      const goalId = nextGoalId++;
      gatherAndMerge(queryObj);

      async function* run(s: Subst, queryObj: Record<string, Term>, pattern: Pattern, walkedQ: Record<string, Term>) {
        const { whereCols, selectCols } = pattern;
        const { whereClauses } = await buildQueryParts(queryObj, s);
        const cacheKey = makeCacheKey(pattern.table, Object.keys(selectCols || {}), whereClauses);
        const rowKey = allParamsGrounded(walkedQ) ? makeRowCacheKey(pattern.table, walkedQ) : null;
        let rows;
        let cacheType = null;
        let matchingPatternGoals = null;

        log("RUN_START", {
          pattern,
          queryObj,
          walkedQ,
        });

        // If the pattern has already been run, use its rows directly
        if (PATTERN_CACHE_ENABLED && pattern.ran) {
          rows = pattern.rows;
          cacheType = 'pattern';
          log("PATTERN_CACHE_HIT", {
            pattern,
            rows,
          });
        }

        // Update pattern cache logic to check all patterns
        if (!rows && PATTERN_CACHE_ENABLED) {
          const matchingPattern = patterns.find(otherPattern => {
            // Check if whereCols match and selectCols are not grounded
            return (
              JSON.stringify(otherPattern.whereCols) === JSON.stringify(pattern.whereCols) && otherPattern.ran === true
            );
          });

          if (matchingPattern) {
            if(matchingPattern.selectCols.length === 0) {
              if (matchingPattern.rows[0] === true) {
                rows = ["HERE"]
              } else {
                rows = []
              }
            } else {
              rows = matchingPattern.rows;
            }
            cacheType = 'pattern';
            matchingPatternGoals = matchingPattern.goalIds;
            log("PATTERN_CACHE_HIT", {
              pattern,
              matchingPattern,
              rows,
            });
          }
        }

        // If all params are grounded, try row cache first
        if (!rows && ROW_CACHE_ENABLED && rowKey) {
          if (rowCache.has(rowKey)) {
            rows = [rowCache.get(rowKey)];
            cacheType = 'row';
            log("ROW_CACHE_HIT", {
              rowKey,
              rows,
            });
          }
        }

        // Otherwise, try exact query cache
        if (!rows && RECORD_CACHE_ENABLED && recordCache.has(cacheKey)) {
          rows = recordCache.get(cacheKey);
          cacheType = 'query';
          log("QUERY_CACHE_HIT", {
            cacheKey,
            rows,
          });
        }

        // Otherwise, hit the DB
        if (!rows) {
          let k = db(pattern.table).select(Object.keys(selectCols || {}));
          for (const [col, val] of Object.entries(whereCols)) {
            k = k.where(col, val as any);
          }
          if (Object.keys(selectCols).length === 0) {
            // Confirmation query: select a static value
            k = db(pattern.table).select(db.raw('1'));
            for (const clause of whereClauses) {
              k = k.where(clause.col, clause.val as any);
            }
          } else {
            k = db(pattern.table).select(Object.keys(selectCols));
            for (const [col, val] of Object.entries(whereCols)) {
              k = k.where(col, val as any);
            }
          }
          rows = await k;
          pattern.ran = true;
          recordCache.set(cacheKey, rows);

          log("DB_QUERY", {
            sql: k.toString(),
            rows,
          });

          // If all params grounded and single row, cache in rowCache
          if (allParamsGrounded(walkedQ) && rows.length === 1) {
            const rowKey = makeRowCacheKey(pattern.table, walkedQ);
            rowCache.set(rowKey, rows[0]);
            log("ROW_CACHE_SET", {
              rowKey,
              row: rows[0],
            });
          }

          // Log real query
          const sql = k.toString();
          queries.push(sql);
          realQueries.push(sql);
          pattern.queries.push(sql);
        } else {
          // Log cache hit
          let desc = '';
          if (cacheType === 'pattern') {
            desc = `[PATTERN CACHE] ${pattern.table} goalIds=${matchingPatternGoals} rows=${JSON.stringify(rows)}`;
          } else if (cacheType === 'row') {
            desc = `[ROW CACHE] ${pattern.table} ${JSON.stringify(walkedQ)}`;
          } else if (cacheType === 'query') {
            desc = `[QUERY CACHE] ${pattern.table} select=${JSON.stringify(Object.keys(selectCols || {}))} where=${JSON.stringify(whereClauses)}`;
          } else {
            desc = `[CACHE] ${pattern.table}`;
          }
          queries.push(desc);
          cacheQueries.push(desc);
          pattern.queries.push(desc);
          log("CACHE_HIT", {
            desc,
          });
        }

        // Update the pattern with the rows returned
        if(rows.length === 1 && (
          rows[0] === true
          || rows[0] === false
        )) {
          //pass
        } else {
          if (Object.keys(selectCols).length === 0) {
          // Confirmation query
            pattern.rows = rows.length > 0 ? [true] : [false];
          } else {
            pattern.rows = rows.length > 0 ? rows : [false];
          }
        }

        log("PATTERN_ROWS_UPDATED", {
          pattern,
        });

        for (const row of pattern.rows) {
          if (row === false) {
            // Skip if no rows were returned
            continue;
          } else if (row === true) {
            // Confirmation query: unify queryObj with whereCols
            const unifiedSubst = await unifyRowWithWalkedQ(Object.keys(whereCols), whereCols, queryObj, s);
            if (unifiedSubst) {
              yield unifiedSubst;
            }
          } else {
            // Regular row processing
            const unifiedSubst = await unifyRowWithWalkedQ(Object.keys(selectCols), walkedQ, row, s);
            if (unifiedSubst) {
              // Invoke mergePatterns after unification to update patterns with newly grounded terms
              const updatedWalkedQ = await walkAllKeys(queryObj, unifiedSubst);
              await mergePatterns(queryObj, updatedWalkedQ, goalId);

              yield unifiedSubst;
            }
          }
        }

        log("RUN_END", {
          pattern,
        });
      };
      
      function gatherAndMerge(queryObj: Record<string, Term>) {
        // Find patterns that match the current queryObj
        // const matches = patterns.filter(pattern => {
        //   // Check if all keys and values in queryObj match the pattern's selectCols
        //   for (const [key, value] of Object.entries(queryObj)) {
        //     const patternValue = pattern.selectCols?.[key];

        //     // If both are logic variables, check if their IDs match
        //     if (isVar(value) && isVar(patternValue)) {
        //       if (value.id !== patternValue.id) {
        //         return false;
        //       }
        //     } else if (value !== patternValue) {
        //       // Otherwise, check for direct equality
        //       return false;
        //     }
        //   }
        //   return true;
        // });

        // if (matches.length > 0) {
        //   // Log matched patterns for debugging
        //   log("MATCHED_PATTERNS", {
        //     matches,
        //   });

        //   // Merge the current goalId into the matching patterns
        //   for (const match of matches) {
        //     if (!match.goalIds.includes(goalId)) {
        //       log("MERGING_PATTERNS", {
        //         match,
        //         goalId 
        //       });
        //       match.goalIds.push(goalId);
        //     }
        //   }
        // } else {
        // Log unmatched queryObj for debugging
        log("UNMATCHED_QUERYOBJ", {
          queryObj,
        });

        // Separate queryObj into selectCols and whereCols
        const selectCols: Record<string, Term> = {};
        const whereCols: Record<string, Term> = {};

        for (const [key, value] of Object.entries(queryObj)) {
          if (isVar(value)) {
            selectCols[key] = value;
          } else {
            whereCols[key] = value;
          }
        }

        // Add `last` property to patterns during initialization with proper formatting
        patterns.push({
          table,
          selectCols,
          whereCols,
          goalIds: [goalId],
          rows: [],
          ran: false,
          last: {
            selectCols: [],
            whereCols: [],
          },
          queries: [],
        });
        
        // }
        
      };

      async function mergePatterns(queryObj: Record<string, Term>, walkedQ: Record<string, Term>, goalId: number) {
        const updatedPatterns: Pattern[] = [];

        for (const pattern of patterns) {
          log("MERGE_PATTERNS_START", {
            pattern,
            goalId,
          });

          // Skip patterns that do not match the current goalId
          if (!pattern.goalIds.includes(goalId)) {
            log("SKIPPED_PATTERN", {
              pattern,
              reason: "GoalId does not match",
              goalId,
            });
            updatedPatterns.push(pattern);
            continue;
          }

          // Skip patterns that have already been run
          if (pattern.ran) {
            log("SKIPPED_PATTERN", {
              pattern,
              reason: "Pattern already ran",
            });
            updatedPatterns.push(pattern);
            continue;
          }

          // Ensure patterns with different selectCols are not merged
          const matchingPatterns = patterns.filter(otherPattern => {
            return (
              otherPattern !== pattern &&
              otherPattern.goalIds.includes(goalId) &&
              JSON.stringify(Object.keys(otherPattern.selectCols).sort()) === JSON.stringify(Object.keys(pattern.selectCols).sort())
            );
          });

          if (matchingPatterns.length > 0) {
            log("MERGING_PATTERNS", {
              matchingPatterns,
            });

            // Merge values from matching patterns
            for (const match of matchingPatterns) {
              for (const key of Object.keys(match.selectCols)) {
                if (isVar(pattern.selectCols[key]) && !isVar(match.selectCols[key])) {
                  log("GROUNDING_SELECT_COL_DURING_MERGE", {
                    key,
                    currentValue: pattern.selectCols[key],
                    newValue: match.selectCols[key],
                  });
                  pattern.selectCols[key] = match.selectCols[key];
                }
              }

              for (const key of Object.keys(match.whereCols)) {
                if (isVar(pattern.whereCols[key]) && !isVar(match.whereCols[key])) {
                  log("GROUNDING_WHERE_COL_DURING_MERGE", {
                    key,
                    currentValue: pattern.whereCols[key],
                    newValue: match.whereCols[key],
                  });
                  pattern.whereCols[key] = match.whereCols[key];
                }
              }

              // Merge goalIds
              match.goalIds.forEach(id => {
                if (!pattern.goalIds.includes(id)) {
                  pattern.goalIds.push(id);
                }
              });
            }
          }

          updatedPatterns.push(pattern);
        }

        log("MERGE_PATTERNS_END", {
          updatedPatterns,
        });

        // Replace the original patterns with the updated ones
        patterns.length = 0;
        patterns.push(...updatedPatterns);
      }

      return async function* factsSql(s: Subst) {
        // console.log("IN GOAL", goalId, queryObj);
        if (s === null) {
          EOSseen(`facts-sql rel ${goalId}`);
          yield null;
          return;
        }

        if (patterns.length === 0) {
          console.log("NO PATTERNS");
          return;
        }

        // Walk queryObj terms and pass to mergePatterns
        const walkedQ = await walkAllKeys(queryObj, s);
        await mergePatterns(queryObj, walkedQ, goalId);

        log("PATTERNS BEFORE", {
          patterns,
        });

        for (const pattern of patterns) {
          if (!pattern.goalIds.includes(goalId)) continue;
          let s2 = s;
          for await (s2 of run(s2, queryObj, pattern, walkedQ)) {
            yield s2;
          }
        }
        log("PATTERNS AFTER", {
          patterns,
        });

        const ranFalsePatterns = patterns.filter(x => x.ran === false);
        if(ranFalsePatterns.length > 0) {
          log("RAN FALSE PATTERNS", {
            ranFalsePatterns 
          });
        }

        const allSelectColsAreTags = (cols: Record<string, Term>): boolean => {
          return Object.values(cols).every((x: Term) => (x as any).id);
        };
        const selectColsMismatchPatterns = patterns.filter(x => x.rows.length > 1 && !allSelectColsAreTags(x.selectCols));
        if(selectColsMismatchPatterns.length > 0) {
          log("SELECTCOLS MISMATCH PATTERNS", {
            selectColsMismatchPatterns 
          });
        }

        const mergedPatterns = patterns.filter(x => x.goalIds.length > 1);
        if(mergedPatterns.length > 0) {
          log("MERGED PATTERNS SEEN. GOOD!", {
            mergedPatterns 
          });
        }

        setTimeout(() => {
          if (goalId === nextGoalId - 1) {
            log("FINAL PATTERNS", {
              patterns,
              goalId, 
            })
          }
        }, 500);

        return;
      }
   
    };
  };

  // --- Symmetric SQL relation (unchanged, but simplified) ---
  const relSym = async (table: string, keys: [string, string]) => {
    let nextGoalId = 1;
    interface Pattern {
      table: string;
      goalIds: number[];
      rows: any[];
      ran: boolean;
      queryObj: Record<string, Term<string | number>>;
      queries: string[];
    }
    const patterns: Pattern[] = [];

    return function goal(queryObj: Record<string, Term<string | number>>) {
      const goalId = nextGoalId++;
      gatherAndMerge(queryObj);

      async function* run(s: Subst, queryObj: Record<string, Term<string | number>>, pattern: Pattern) {
        const values = Object.values(queryObj);
        if (s === null) {
          EOSseen("facts-sql relSym");
          yield null;
          return;
        }
        if (values.length > 2) return;

        const walkedValues: Term[] = await Promise.all(values.map(x => walk(x, s)));
        if (walkedValues[0] === walkedValues[1]) return;

        const gv = walkedValues.filter(x => !isVar(x)) as (string | number)[];
        let rows;
        let cacheType;
        let matchingPatternGoals;

        log("RUN_START", {
          pattern,
          queryObj,
          walkedValues,
        });

        // If the pattern has already been run, use its rows directly
        if (PATTERN_CACHE_ENABLED && pattern.ran) {
          rows = pattern.rows;
          log("PATTERN_CACHE_HIT", {
            pattern,
            rows,
          });
        }

        // Update pattern cache logic to check all patterns
        if (!rows && PATTERN_CACHE_ENABLED) {
          const matchingPattern = patterns.find(otherPattern => {
            // Check if whereCols match and selectCols are not grounded
            return (
              JSON.stringify(otherPattern.whereCols) === JSON.stringify(pattern.whereCols) && otherPattern.ran === true
            );
          });

          if (matchingPattern) {
            if(matchingPattern.selectCols.length === 0) {
              if (matchingPattern.rows[0] === true) {
                rows = ["HERE"]
              } else {
                rows = []
              }
            } else {
              rows = matchingPattern.rows;
            }
            cacheType = 'pattern';
            matchingPatternGoals = matchingPattern.goalIds;
            log("PATTERN_CACHE_HIT", {
              pattern,
              matchingPattern,
              rows,
            });
          }
        }

        // Otherwise, hit the DB
        if (!rows) {
          let k;
          if (gv.length === 2) {
            k = db(table).select(keys)
              .where(keys[0], gv[0])
              .andWhere(keys[1], gv[1]);
          } else {
            k = db(table).select(keys).where(keys[0], gv[0]).orWhere(keys[1], gv[0]);
          }
          // Log real query
          const sql = k.toString();
          rows = await k;
          pattern.ran = true;

          log("DB_QUERY", {
            sql,
            rows,
          });

          realQueries.push(sql);
          pattern.queries.push(sql);
        } else {

          // Log cache hit
          let desc = '';
          if (cacheType === 'pattern') {
            desc = `[PATTERN CACHE] ${pattern.table} goalIds=${matchingPatternGoals} rows=${JSON.stringify(rows)}`;
          } else {
            desc = `[CACHE] ${pattern.table}`;
          }
          queries.push(desc);
          cacheQueries.push(desc);
          pattern.queries.push(desc);
          log("CACHE_HIT", {
            desc,
          });

        }

        // Update the pattern with the rows returned
        pattern.rows = rows;

        log("PATTERN_ROWS_UPDATED", {
          pattern,
        });

        for (const row of rows) {
          const s2 = new Map(s);
          const unified1 = await unify(walkedValues[0], row[keys[0]], s2);
          if (unified1) {
            const unified2 = await unify(walkedValues[1], row[keys[1]], unified1);
            if (unified2) {
              yield unified2;
              continue;
            }
          }
          const s3 = new Map(s);
          const unified3 = await unify(walkedValues[1], row[keys[0]], s3);
          if (unified3) {
            const unified4 = await unify(walkedValues[0], row[keys[1]], unified3);
            if (unified4) {
              yield unified4;
            }
          }
        }

        log("RUN_END", {
          pattern,
        });
      }

      function gatherAndMerge(queryObj: Record<string, Term<string | number>>) {
        // Normalize queryObj to account for symmetric keys
        const normalizedQueryObj = Object.values(queryObj);
        const whereCols = normalizedQueryObj.filter(x => !isVar(x));
        const selectCols = normalizedQueryObj.filter(x => isVar(x));


        patterns.push({
          table,
          selectCols,
          whereCols,
          goalIds: [goalId],
          rows: [],
          ran: false,
          last: {
            selectCols: [],
            whereCols: [],
          },
          queries: [],
        });
        
      }

      return async function* factsSqlSym(s: Subst) {
        if (s === null) {
          EOSseen(`facts-sql relSym ${goalId}`);
          yield null;
          return;
        }

        if (patterns.length === 0) {
          console.log("NO PATTERNS");
          return;
        }

        log("PATTERNS BEFORE", {
          patterns,
        });

        for (const pattern of patterns) {
          if (!pattern.goalIds.includes(goalId)) continue;
          let s2 = s;
          for await (const result of run(s2, queryObj, pattern)) {
            if (result === null) continue;
            s2 = result;
            yield s2;
          }
        }

        log("PATTERNS AFTER", {
          patterns,
        });

        setTimeout(() => {
          if (goalId === nextGoalId - 1) {
            log("FINAL PATTERNS SYM", {
              patterns,
              goalId, 
            })
          }
        }, 500);

        return;
      };
    };
  };

  return {
    rel,
    relSym,
    db,
    queries,
    realQueries,
    cacheQueries,
  };
};
