// @ts-expect-error
import importNewlines from "eslint-plugin-import-newlines";
import importPlugin from 'eslint-plugin-import';
import js from '@eslint/js';
import stylistic from '@stylistic/eslint-plugin'
import tseslint from "typescript-eslint";

export default tseslint.config(
  js.configs.recommended,
  // tseslint.configs.stylistic,
  // tseslint.configs.strictTypeChecked,
  tseslint.configs.stylisticTypeChecked,
  importPlugin.flatConfigs.recommended,
  {
    files: ['**/*.{js, ts,tsx}'],
    extends: [importPlugin.flatConfigs.recommended, importPlugin.flatConfigs.typescript],
    rules: {
      "import/no-unresolved": ["off"],
    },
  },
  {
    settings: {
      "import/parsers": {
        "@typescript-eslint/parser": [".ts", ".tsx"],
      },
    },
    languageOptions: {
      parserOptions: {
        project: true,
        sourceType: "module",
      },
    },
    "plugins": {
      "import-newlines": importNewlines,
      "@stylistic":  stylistic ,

    },
    rules: {
      "computed-property-spacing": ["error", "never"],
      "comma-dangle": ["error", {
        "arrays": "only-multiline",
        "objects": "only-multiline",
        "imports": "only-multiline",
        "exports": "only-multiline",
        "functions": "only-multiline",
      }],
      "@stylistic/no-multi-spaces": ["error"],
      "no-unused-vars": ["off"],
      indent: ["error", 2],
      // "sort-imports": ["error", {
      //   allowSeparatedGroups: true,
      // }],
      "object-curly-spacing": ["error", "always", {
        "arraysInObjects": false,
        "objectsInObjects": false,
      }],
      "object-curly-newline": [
        "error",
        {
          ObjectExpression: {
            multiline: true,
            minProperties: 1,
          },
        },
      ],
      "object-property-newline": [
        "error",
        // {
        //   allowAllPropertiesOnSameLine: false,
        // },
      ],
      "import/consistent-type-specifier-style": ["error"],
      // "import/exports-last": ["error"],
      "import/first": ["error"],
      "import/newline-after-import": ["error"],
      "import/order": ["error"],
      "import/no-duplicates": ["error"],
      "import-newlines/enforce": "error"
      // You can add more TypeScript-specific rules here
    },
  },
);
