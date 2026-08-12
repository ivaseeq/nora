const path = require('node:path');

module.exports = {
  content: [path.resolve(__dirname, '../../nora-registry/src/ui/**/*.rs')],
  theme: { extend: {} },
  plugins: [],
};
