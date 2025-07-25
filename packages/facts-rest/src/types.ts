import { RelationOptions } from "facts-abstract";

/**
 * REST-specific relation options (extends global RelationOptions)
 */
export interface RestRelationOptions extends RelationOptions {
  pathTemplate?: string;
}
