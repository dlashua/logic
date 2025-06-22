// @ts-check
import eslint from '@eslint/js';
import tseslint from "typescript-eslint";

export default tseslint.config(
//   eslint.configs.recommended,
  tseslint.configs.stylistic,
  {
    languageOptions: {
      parserOptions: {
        project: true,
        sourceType: 'module',
      },
    },
    rules: {
      indent: ['error',
        2],
      //   "sort-imports": ["error"],
      "object-curly-newline": ["error",
        {
          "ObjectExpression": {
            "multiline": true,
            "minProperties": 1,
          },
        }],
      "object-property-newline": ["error",
        {
          "allowAllPropertiesOnSameLine": false,
        }],
      "array-element-newline": ["error",
        "always"],
      "comma-dangle": ["error",
        "always-multiline"],
      // You can add more TypeScript-specific rules here
    },
  },
);
