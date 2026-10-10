TransitGap app — deploy as a Databricks App (Custom).
1. Add a SQL warehouse resource (CAN USE) and copy its HTTP path into app.yaml.
2. Grant the app's service principal USE CATALOG / USE SCHEMA / SELECT on <catalog>.transitgap and CAN RUN on the Genie space.
3. Deploy. If the Genie SDK method names differ in your SDK version, the page shows the error and you can demo Genie in its own tab.
