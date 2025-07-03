import { ConfigurationManager } from "../shared/config.ts";
import { getDefaultLogger } from "../shared/simple-logger.ts";
import { BaseCache } from "../shared/cache.ts";
import { BaseConfig } from "../shared/types.ts";
import { FactRelationFactory } from "./relation-factory.ts";
import { FactRelation, FactObjRelation, FactRelationConfig } from "./types.ts";

export const makeFacts = (
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig
) => {
  const configuration = ConfigurationManager.create(config);
  const logger = getDefaultLogger();
  const cache = new BaseCache(configuration.cache, logger);
  
  const factory = new FactRelationFactory({
    logger,
    cache,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true 
    }
  });

  return factory.createArrayRelation();
};

export const makeFactsObj = (
  keys: string[],
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig
): FactObjRelation => {
  const configuration = ConfigurationManager.create(config);
  const logger = getDefaultLogger();
  const cache = new BaseCache(configuration.cache, logger);
  
  const factory = new FactRelationFactory({
    logger,
    cache,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true 
    }
  });

  return factory.createObjectRelation(keys);
};

export const makeFactsSym = (
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig
): FactRelation => {
  const configuration = ConfigurationManager.create(config);
  const logger = getDefaultLogger();
  const cache = new BaseCache(configuration.cache, logger);
  
  const factory = new FactRelationFactory({
    logger,
    cache,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true 
    }
  });

  return factory.createSymmetricArrayRelation();
};

export const makeFactsObjSym = (
  keys: string[],
  config?: Partial<BaseConfig>,
  factConfig?: FactRelationConfig
): FactObjRelation => {
  const configuration = ConfigurationManager.create(config);
  const logger = getDefaultLogger();
  const cache = new BaseCache(configuration.cache, logger);
  
  const factory = new FactRelationFactory({
    logger,
    cache,
    config: factConfig || {
      enableLogging: false,
      enableIndexing: true 
    }
  });

  return factory.createSymmetricObjectRelation(keys);
};

