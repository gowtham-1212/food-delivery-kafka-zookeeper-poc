FROM node:20-slim

WORKDIR /app

COPY frontend-dashboard/package.json ./
RUN npm install

COPY frontend-dashboard/ ./

CMD ["npm", "run", "dev"]
