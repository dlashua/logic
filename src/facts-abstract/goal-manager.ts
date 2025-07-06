import type { Term } from "../core/types.ts";
import type { GoalRecord, GoalManager } from "./types.ts";

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

  addGoal(goalId: number, table: string, queryObj: Record<string, Term>, batchKey?: string): void {
    this.goals.push({
      goalId,
      table,
      queryObj,
      batchKey
    });
  }

  getGoalById(id: number): GoalRecord | undefined {
    return this.goals.find(goal => goal.goalId === id);
  }

  getGoalsByBatchKey(batchKey: string): GoalRecord[] {
    return this.goals.filter(goal => goal.batchKey === batchKey);
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