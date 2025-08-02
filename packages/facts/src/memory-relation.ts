import { mergeMap } from "rxjs/operators";
import { from, EMPTY, Observable } from "rxjs";
import {
  type Goal,
  indexUtils,
  isVar,
  type Logger,
  type Subst,
  type Term,
  unify,
} from "@codespiral/logic";
import type { FactRelation, FactRelationConfig } from "./types.js";

export class MemoryRelation {
  private facts: Term[][] = [];
  private indexes = new Map<number, Map<unknown, Set<number>>>();

  constructor(
    private logger: Logger,
    private config: FactRelationConfig,
  ) {}

  createRelation(): FactRelation {
    const goalFn = (...query: Term[]): Goal => {
      return this.createGoal(query);
    };

    goalFn.set = (...fact: Term[]) => {
      this.addFact(fact);
    };

    goalFn.raw = this.facts;
    goalFn.indexes = this.indexes;

    return goalFn;
  }

  private createGoal(query: Term[]): Goal {
    return (input$: Observable<Subst>) => {
      this.logger.log("RUN_START", {
        message: `Starting memory relation query`,
        query,
      });

      // Simplified approach using pure RxJS
      return input$.pipe(
        mergeMap((s: Subst) => {
          const results = this.queryFacts(query, s);
          return results.length > 0 ? from(results) : EMPTY;
        }),
      );
    };
  }

  private queryFacts(query: Term[], substitution: Subst): Subst[] {
    const results: Subst[] = [];

    // Try to use indexes for optimization
    const candidateIndexes = this.getCandidateIndexes(query);

    let factsToCheck: Term[][];
    if (candidateIndexes.size === 0) {
      // Full scan
      this.logger.log("FULL_SCAN", {
        message: `Full scan of ${this.facts.length} facts`,
      });
      factsToCheck = this.facts;
    } else {
      // Use indexed facts
      this.logger.log("INDEX_LOOKUP", {
        message: `Checking ${candidateIndexes.size} indexed facts`,
      });
      factsToCheck = Array.from(candidateIndexes).map((i) => this.facts[i]);
    }

    // Check each fact against the query
    for (const fact of factsToCheck) {
      const unificationResult = unify(query, fact, substitution);
      if (unificationResult) {
        this.logger.log("FACT_MATCH", {
          message: "Fact matched",
          fact,
          query,
        });
        results.push(unificationResult);
      }
    }

    return results;
  }

  private getCandidateIndexes(query: Term[]): Set<number> {
    const indexedPositions: number[] = [];
    query.forEach((term, i) => {
      if (!isVar(term) && this.indexes.has(i)) {
        indexedPositions.push(i);
      }
    });

    let candidateIndexes: Set<number> = new Set();

    if (indexedPositions.length > 0) {
      this.logger.log("INDEX_LOOKUP", {
        message: `Using indexes for positions: ${indexedPositions.join(", ")}`,
      });

      for (const pos of indexedPositions) {
        const term = query[pos];
        const index = this.indexes.get(pos);
        if (!index) continue;

        const factNums = index.get(term);
        if (!factNums || factNums.size === 0) {
          candidateIndexes = new Set();
          break;
        }

        if (
          candidateIndexes.size === 0 &&
          indexedPositions.indexOf(pos) === 0
        ) {
          candidateIndexes = new Set(factNums);
        } else {
          candidateIndexes = indexUtils.intersect(candidateIndexes, factNums);
          if (candidateIndexes.size === 0) break;
        }
      }
    }

    return candidateIndexes;
  }

  private addFact(fact: Term[]): void {
    const factIndex = this.facts.length;
    this.facts.push(fact);

    if (this.config.enableIndexing !== false) {
      fact.forEach((term, position) => {
        if (indexUtils.isIndexable(term)) {
          let index = this.indexes.get(position);
          if (!index) {
            index = indexUtils.createIndex();
            this.indexes.set(position, index);
          }
          indexUtils.addToIndex(index, term, factIndex);
        }
      });
    }

    this.logger.log("FACT_ADDED", {
      message: `Added fact at index ${factIndex}`,
      fact,
    });
  }
}
