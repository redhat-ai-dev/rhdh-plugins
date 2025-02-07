# [Backstage](https://backstage.io)

This is your newly scaffolded Backstage App, Good Luck!

To start the app, run:

```sh
yarn install
yarn dev
```

To generate knip reports for this app, run:

```sh
yarn backstage-repo-tools knip-reports
```

## Running the moduile

To run the rhdh-ai backend module in development mode:

1. Ensure the following environment variables are set:

- `RHDH_AI_BRIDGE_SERVER`: Set to the server hosting the RHDH AI Bridge
- `RHDH_BRIDGE_HOST`: Similar as above, but without the protocol
- `RHDH_TOKEN`: Any 8 character password to use for authentication with RHDH

2. Run `yarn install` to install dependencies

3. Run `yarn dev` to launch the module
