const config = {
    '!(**/__generated/**/*)*.{ts,tsx,graphql}': () => ['pnpm run tsc'],
    '!(**/__generated/**/*)!(**/types/**/*)*.{ts,tsx,graphql,cjs,mjs,js,jsx}': [
        'oxlint --fix --max-warnings 0',
        'oxfmt --write',
    ],
    '!(**/__generated/**/*)*.{html,json,md,yaml,yml}': ['oxfmt --write'],
};
export default config;
