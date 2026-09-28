# syntax=docker/dockerfile:1

# ---- base: versao do Node e arquivos de dependencia --------------------------
FROM node:24-alpine AS base
WORKDIR /app
RUN apk add --no-cache libc6-compat
COPY package.json yarn.lock ./
COPY patches ./patches

# ---- deps: todas as dependencias (build e migrate) ---------------------------
# --ignore-scripts: nenhum script de pacote roda na instalacao; o unico que o
# projeto precisa (patch no kafkajs) e aplicado explicitamente.
FROM base AS deps
RUN yarn install --frozen-lockfile --ignore-scripts && yarn patch-package

# ---- builder: gera o client Prisma e compila --------------------------------
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN yarn prisma generate && yarn build

# ---- migrate: job de migrations (usa devDependencies: prisma CLI) -----------
# Alvo separado para a imagem final levar so dependencias de producao.
FROM builder AS migrate
USER node
CMD ["yarn", "--silent", "prisma", "migrate", "deploy"]

# ---- prod-deps: so dependencias de producao ---------------------------------
# O Yarn 1 instala mesmo com --production as peerDependencies opcionais do
# @prisma/client (prisma, typescript) quando elas tambem sao devDependencies,
# levando o CLI do Prisma para a imagem final. Sem devDependencies no
# package.json a instalacao e so de producao; as versoes continuam vindo do
# yarn.lock (sem --frozen-lockfile porque o lock ainda lista as de dev).
FROM base AS prod-deps
RUN node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync('package.json','utf8'));delete p.devDependencies;fs.writeFileSync('package.json',JSON.stringify(p,null,2))" && \
    yarn install --production --ignore-scripts --non-interactive && \
    yarn cache clean
# kafkajs com o patch aplicado (patch-package e devDependency).
COPY --from=deps /app/node_modules/kafkajs ./node_modules/kafkajs

# ---- production: runtime minimo, usuario nao-root ---------------------------
FROM node:24-alpine AS production
ENV NODE_ENV=production
WORKDIR /app
RUN apk add --no-cache libc6-compat dumb-init && \
    addgroup -g 1001 nodejs && \
    adduser -S -u 1001 -G nodejs nestjs
COPY --from=prod-deps --chown=nestjs:nodejs /app/node_modules ./node_modules
COPY --from=builder --chown=nestjs:nodejs /app/dist ./dist
COPY --from=builder --chown=nestjs:nodejs /app/package.json ./package.json
USER nestjs
ENV PORT=8080
EXPOSE 8080
# dumb-init como PID 1 repassa o SIGTERM para o Node (graceful shutdown).
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "dist/src/main.js"]
