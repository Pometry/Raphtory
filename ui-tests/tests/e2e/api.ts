const GRAPHQL_URL = process.env.GRAPHQL_URL ?? 'http://localhost:1736';

export async function graphqlMutation<T = unknown>(
    query: string,
    variables?: Record<string, unknown>,
): Promise<T> {
    const response = await fetch(GRAPHQL_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, variables }),
    });
    if (!response.ok) {
        throw new Error(`GraphQL request failed: ${response.statusText}`);
    }
    const result = await response.json();
    if (result.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
    }
    return result.data as T;
}

export async function copyGraph(path: string, newPath: string): Promise<void> {
    await graphqlMutation(
        'mutation($path: String!, $newPath: String!) { copyGraph(path: $path, newPath: $newPath) }',
        { path, newPath },
    );
}

export async function deleteNamespace(path: string): Promise<void> {
    await graphqlMutation(
        'mutation($path: String!) { deleteNamespace(path: $path) }',
        { path },
    );
}
