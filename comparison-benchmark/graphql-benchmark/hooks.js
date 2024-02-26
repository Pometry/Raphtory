module.exports = {
    checkForGraphQLError: (requestParams, response, context, ee, next) => {
        const body = JSON.parse(response.body);
        if (body.errors) {
            // console.error('GraphQL Error:', body.errors);
            ee.emit('error', 'GraphQL Error');
        }
        return next(); // MUST be called for the scenario to continue
    },
};