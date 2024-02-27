module.exports = {
    // beforeScenario: (context, events, done) => {
    //     context.vars.offset = 1;
    //     console.log("Set the offset")
    //     return done();
    // },
    checkForGraphQLError: (requestParams, response, context, ee, next) => {
        const body = JSON.parse(response.body);
        if (body.errors) {
            console.error('GraphQL Error: Query:', requestParams['json']);
            console.error('GraphQL Error body:', body.errors);
            ee.emit('error', 'GraphQL Error');
        }
        return next(); // MUST be called for the scenario to continue
    },
    saveNodeCount: (requestParams, response, context, ee, next) => {
        try {
            const body = JSON.parse(response.body);
            const nodeCount = body.data.subgraph.nodes.page.length;
            context.vars.nodeCount = nodeCount;
        } catch (error) {
            console.log('Error parsing response:', error);
        }
        return next(); // MUST be called for the scenario to continue
    },
    increaseOffset: (requestParams, context, ee, next) => {
        if (context.vars.nodeCount === context.vars.limit) {
            context.vars.offset += context.vars.limit;
        }
        return next();
    },
};