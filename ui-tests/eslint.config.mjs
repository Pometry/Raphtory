import eslint from '@eslint/js';
import { globalIgnores } from 'eslint/config';
import importPlugin from 'eslint-plugin-import';
import react from 'eslint-plugin-react';
import reactRefresh from 'eslint-plugin-react-refresh';
import unusedImports from 'eslint-plugin-unused-imports';
import globals from 'globals';
import tseslint, { configs as tsEslintConfigs } from 'typescript-eslint';

export default tseslint.config(
    eslint.configs.recommended,
    tsEslintConfigs.strict,
    tsEslintConfigs.stylistic,
    [
        {
            languageOptions: {
                globals: {
                    ...globals.browser,
                    ...globals.node,
                },
                ecmaVersion: 'latest',
                sourceType: 'module',
            },

            extends: [
                importPlugin.flatConfigs.recommended,
                importPlugin.flatConfigs.typescript,
            ],
            //     fixupConfigRules(
            //     compat.extends(
            //         'eslint:recommended',
            //         'plugin:@typescript-eslint/recommended',
            //         'plugin:react/recommended',
            //         'plugin:react-hooks/recommended',
            //         'plugin:import/recommended',
            //         'plugin:import/typescript',
            //         'plugin:yml/prettier',
            //     ),
            // ),

            settings: {
                // typescript: true,
                'import/resolver': {
                    typescript: true,
                    node: true,
                },
                react: {
                    version: 'detect',
                },
            },

            plugins: {
                react,
                'react-refresh': reactRefresh,
                'unused-imports': unusedImports,
            },

            rules: {
                'react-refresh/only-export-components': 'warn',
                'import/no-empty-named-blocks': 'error',
                'import/order': [
                    'error',
                    {
                        groups: [
                            ['builtin', 'external', 'object', 'type'],
                            ['internal', 'parent', 'sibling', 'index'],
                        ],

                        alphabetize: {
                            order: 'asc',
                            caseInsensitive: true,
                        },
                    },
                ],
                'sort-imports': [
                    'error',
                    {
                        allowSeparatedGroups: true,
                        ignoreCase: true,
                        ignoreDeclarationSort: true,
                        ignoreMemberSort: false,
                        memberSyntaxSortOrder: [
                            'none',
                            'all',
                            'multiple',
                            'single',
                        ],
                    },
                ],
                '@typescript-eslint/no-unused-vars': 'off',
                '@typescript-eslint/no-explicit-any': 'error',
                '@typescript-eslint/no-non-null-assertion': 'off',
                '@typescript-eslint/no-empty-function': 'off',
                'unused-imports/no-unused-imports': 'error',
                'unused-imports/no-unused-vars': [
                    'warn',
                    {
                        vars: 'all',
                        varsIgnorePattern: '^_',
                        args: 'after-used',
                        argsIgnorePattern: '^_',
                    },
                ],
                'import/no-duplicates': [
                    'error',
                    {
                        considerQueryString: true,
                    },
                ],
                'import/no-unresolved': [
                    'error',
                    {
                        ignore: ['^~icons/'],
                    },
                ],
                'react/react-in-jsx-scope': 'off',
                'react/jsx-key': 'error',
                'react/no-array-index-key': 'warn',
            },
        },
        globalIgnores([
            '!**/.github/',
            '!**/.ladle/',
            '!**/.*.cjs',
            '!**/.*.mjs',
            '!**/.*.json',
            '**/__generated/**/*',
            '**/dist',
            '**/types/',
            '**/playwright-report/**/*',
            '**/blob-report/**/*',
            '**/test-results/**/*',
        ]),
    ],
);
