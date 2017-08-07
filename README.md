# reef-example-nodelabel
This is an example REEF application for demonstrating YARN node labeling.
It is tested on hadoop 2.7.2 and REEF 0.16.0-SNAPSHOT.

### Scenario
This example requires a yarn cluster that contains some slave nodes that are labeled as "mylabel".

`REEFYarnNodeLabelTestDriver` submits two `EvaluatorRequest`s, one is labeled as "mylabel" and the other is not labeled.

After evaluator for each `EvaluatorRequest` is allocated, `EvaluatorAllocatedHandler` records logs showing which node the evaluator is allocated and which label the node has.

### Configuration
./yarnconfig contains YARN configuration files for testing.
