// @ts-expect-error
import importNewlines from "eslint-plugin-import-newlines";
import importPlugin from 'eslint-plugin-import';
import js from '@eslint/js';
import stylistic from '@stylistic/eslint-plugin'
import tseslint from "typescript-eslint";
import { defineConfig, globalIgnores } from "eslint/config";

export default defineConfig(
  globalIgnores([
    "./hold/**",
    "./extra/**",
  ]),
  js.configs.recommended,
  //@ts-expect-error
  tseslint.configs.stylistic,
  importPlugin.flatConfigs.recommended,
  {
    basePath: "./",
    files: ['./**/*.{js,ts,tsx}'],
    extends: [importPlugin.flatConfigs.recommended, importPlugin.flatConfigs.typescript],
    settings: {
      "import/parsers": {
        "@typescript-eslint/parser": [".ts", ".tsx"]
      }
    },
    languageOptions: {
      parserOptions: {
        project: true,
        sourceType: "module",
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    "plugins": {
      "import-newlines": importNewlines,
      "@stylistic":  stylistic ,

    },
    rules: {
      
      // delete these later
      "@typescript-eslint/prefer-for-of": ["off"],

      // leave these on
      "lines-around-comment": ["error"],
      "import/no-unresolved": ["off"],
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
            minProperties: 4,
          },
        },
      ],
      "object-property-newline": [
        "error",
        {
          allowAllPropertiesOnSameLine: true,
        },
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
