import { RelationOptions } from "../facts-abstract/types.ts";

/**
 * REST-specific relation options (extends global RelationOptions)
 */
export interface RestRelationOptions extends RelationOptions {
  pathTemplate?: string;
}
