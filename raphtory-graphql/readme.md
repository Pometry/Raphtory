
# Raphtory-GraphQL

## Overview

Raphtory-GraphQL is part of the Raphtory project, an in-memory vectorized graph database designed for high performance and scalability. This module provides GraphQL support for Raphtory, allowing users to interact with their graph data through GraphQL queries.

## Features

- **In-Memory Graph Database:** Offers high-speed data processing and querying capabilities.
- **GraphQL Integration:** Allows seamless integration of graph data with web applications through a GraphQL API.
- **Authentication Support:** Includes options to run the server with authentication, ensuring secure access to the graph data.

## Installation

Clone the repository and navigate to the `raphtory-graphql` directory:
```bash
git clone https://github.com/Pometry/Raphtory.git
cd Raphtory/raphtory-graphql
```

## Configuration

Ensure you have the required environment variables set up. For example, set the `GRAPH_DIRECTORY` environment variable:
```bash
export GRAPH_DIRECTORY=/path/to/your/graph_directory
```

Create a `config.toml` file with your specific configuration settings.

## Running the Server

By default, the server runs without authentication. To run the server, use the following command:
```bash
cargo run
```

This command starts the Raphtory server using `from_directory.run`.

## Running the Server with Authentication (Microsoft)

### Setting up Authentication

To enable authentication for the Raphtory-GraphQL server, you need to set up a `.env` file with specific properties from Microsoft. This file should include the following properties:

- `CLIENT_ID`
- `CLIENT_SECRET`
- `TENANT_ID`
- `AUTHORITY`

#### Steps 

1. **Azure Portal Registration:**
    - Go to the [Azure Portal](https://portal.azure.com/).
    - Navigate to "Azure Active Directory" in the left-hand menu.

2. **Register a New Application:**
    - Click on "App registrations" and then "New registration."
    - Enter a name for your application.
    - Select the supported account types (typically "Accounts in this organizational directory only").
    - Click "Register."

3. **Get the Client ID and Tenant ID:**
    - After registration, you will be taken to the application's overview page.
    - Copy the `Application (client) ID` and `Directory (tenant) ID` values. These are your `CLIENT_ID` and `TENANT_ID`, respectively.

4. **Create a Client Secret:**
    - In the left-hand menu, select "Certificates & secrets."
    - Click on "New client secret."
    - Provide a description and set an expiry period.
    - Click "Add."
    - Copy the value of the client secret. This is your `CLIENT_SECRET`.

5. **Set the Authority:**
    - The `AUTHORITY` is typically in the format `https://login.microsoftonline.com/{TENANT_ID}`.
  
6. **Set the redirection URLS**
    - Next you need to set the redirection URLs, Go to the Manage > Authentication and add the following, note you can change `http://localhost:1736` to a custom url if it is different
    - "http://localhost:1736/"
    - "http://localhost:1736/auth/callback"
  
7. **Set some permissions**
    - Next we need to set some permissions onto the application so we able to use it.
    - Go to Manage > Expose an API > Add a scope
          - Set Scope NAme, Admin Consent Display name and Admin consent description to "public-scope" without quotes
          - Set Who can consent? To Admin and Users,
          - Click Add Scope
   - Go to Manage > API Permissions. Then remove any existing permissions include the Microsoft Graph default permissions.
   - Now the next step, if you just made the scope it may not show up and can take a while, wait 10-20 mins, refresh the page and return if you do not see the app
   - Click Add a permission > Under APIs my organization uses > type in the name of your app, and click on the name, you will see it comes up with a "Select permissions" page, select the "public-scope" permission we just made and finally click "Add permissions" on the bottom of the page 

#### Example .env File

Create a `.env` file in the root directory of your project and add the obtained properties:

```env
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret
TENANT_ID=your_tenant_id
AUTHORITY=https://login.microsoftonline.com/your_tenant_id
```

Ensure that this file is included in your `.gitignore` to prevent sensitive information from being exposed.

With these settings configured, your Raphtory-GraphQL server will be able to use Microsoft authentication.

### Running the Auth server

To run the server with authentication, pass the `--server` argument:
```bash
cargo run -- --server
```

This command starts the Raphtory server using `run_with_auth`, which includes authentication mechanisms to secure access.
