import { type BaseConfig, getDefaultLogger } from "@swiftfall/logic";
import { FactRelationFactory } from "./relation-factory.js";
import type {
  FactObjRelation,
  FactRelation,
  FactRelationConfig,
} from "./types.js";

export const makeFacts = (
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig,
) => {
  const logger = getDefaultLogger();

  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true,
    },
  });

  return factory.createArrayRelation();
};

export const makeFactsObj = (
  keys: string[],
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig,
): FactObjRelation => {
  const logger = getDefaultLogger();

  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true,
    },
  });

  return factory.createObjectRelation(keys);
};

export const makeFactsSym = (
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig,
): FactRelation => {
  const logger = getDefaultLogger();

  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true,
    },
  });

  return factory.createSymmetricArrayRelation();
};

export const makeFactsObjSym = (
  keys: string[],
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig,
): FactObjRelation => {
  const logger = getDefaultLogger();

  const factory = new FactRelationFactory({
    logger,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true,
    },
  });

  return factory.createSymmetricObjectRelation(keys);
};
