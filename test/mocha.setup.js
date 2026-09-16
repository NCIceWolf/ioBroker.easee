// Don't silently swallow unhandled rejections
process.on("unhandledRejection", (error) => {
  throw error;
});

const chai = require("chai");
const sinonChai = require("sinon-chai").default;
const chaiAsPromised = require("chai-as-promised").default;

chai.use(sinonChai);
chai.use(chaiAsPromised);
chai.should();
