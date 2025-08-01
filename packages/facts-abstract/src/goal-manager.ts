import type { Term } from "@swiftfall/logic";
import type { GoalManager, GoalRecord } from "./types.js";

/**
 * Default implementation of GoalManager
 * Handles goal tracking, ID generation, and query logging
 */
export class DefaultGoalManager implements GoalManager {
  private goals: GoalRecord[] = [];
  private queries: string[] = [];
  private nextGoalId = 0;

  getNextGoalId(): number {
    return ++this.nextGoalId;
  }

  addGoal(
    goalId: number,
    relationIdentifier: string,
    queryObj: Record<string, Term>,
    batchKey?: string,
    relationOptions?: any,
  ): void {
    this.goals.push({
      goalId,
      relationIdentifier,
      queryObj,
      batchKey,
      relationOptions,
    });
  }

  getGoalById(id: number): GoalRecord | undefined {
    return this.goals.find((goal) => goal.goalId === id);
  }

  getGoalsByBatchKey(batchKey: string): GoalRecord[] {
    return this.goals.filter((goal) => goal.batchKey === batchKey);
  }

  getGoals(): GoalRecord[] {
    return [...this.goals];
  }

  clearGoals(): void {
    this.goals.length = 0;
  }

  addQuery(query: string): void {
    this.queries.push(query);
  }

  getQueries(): string[] {
    return [...this.queries];
  }

  clearQueries(): void {
    this.queries.length = 0;
  }

  getQueryCount(): number {
    return this.queries.length;
  }
}
