The tool for putting NEAR blockchain data into AWS RedShift.

### FAQ

#### Why so complex?

Because RedShift does not have any constraints.
The only way to achieve uniqueness is to check the data carefully not to load the duplicates.

#### Can it break the Aurora DB?

No, we can only read from external tables, we can't modify anything.
