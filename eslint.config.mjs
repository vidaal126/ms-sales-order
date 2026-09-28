// @ts-check
import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: [
      "dist/**",
      "coverage/**",
      "node_modules/**",
      "src/infrastructure/database/generated/**",
      "eslint.config.mjs",
    ],
  },
  eslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/ban-ts-comment": "error",
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": "error",
      "@typescript-eslint/explicit-function-return-type": [
        "error",
        { allowExpressions: true, allowTypedFunctionExpressions: true },
      ],
      // Classes do Nest (modulos, DTOs) sao vazias ou so com decorators.
      "@typescript-eslint/no-extraneous-class": ["error", { allowWithDecorator: true }],
      "@typescript-eslint/no-unused-vars": ["error", { ignoreRestSiblings: true, argsIgnorePattern: "^_" }],
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        { allowNumber: true, allowBoolean: true },
      ],
    },
  },
  {
    // jest: expect(obj.method).toHaveBeenCalled() referencia metodo sem this.
    files: ["**/*.spec.ts", "**/*.int-spec.ts", "**/*.e2e-spec.ts", "test/**/*.ts", "src/test/**/*.ts"],
    rules: {
      "@typescript-eslint/unbound-method": "off",
      "@typescript-eslint/require-await": "off",
      // Fakes replicam as assinaturas genericas do Nest (getRequest<T>()).
      "@typescript-eslint/no-unnecessary-type-parameters": "off",
      // Teardown precisa tolerar um beforeAll que falhou no meio (container
      // declarado como nao-nulo, mas ainda nao iniciado).
      "@typescript-eslint/no-unnecessary-condition": "off",
    },
  },
);
