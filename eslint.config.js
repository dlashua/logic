// @ts-check
import eslint from '@eslint/js';
import tseslintPlugin from '@typescript-eslint/eslint-plugin';
import tseslintParser from '@typescript-eslint/parser';

export default [
    eslint.configs.recommended,
    {
        plugins: {
            '@typescript-eslint': tseslintPlugin
        },
        languageOptions: {
            parser: tseslintParser,
            parserOptions: {
                project: './tsconfig.json',
                sourceType: 'module',
            },
        },
        rules: {
            indent: ['error', 4],
            // You can add more TypeScript-specific rules here
        },
    },
];
