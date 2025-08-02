import type { RelationOptions } from "@codespiral/facts-abstract";

/**
 * REST-specific relation options (extends global RelationOptions)
 */
export interface RestRelationOptions extends RelationOptions {
  pathTemplate?: string;
}
