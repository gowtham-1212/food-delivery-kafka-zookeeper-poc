FROM node:20-slim

WORKDIR /app

COPY services/kitchen-service/package.json ./
RUN npm install

COPY services/kitchen-service/ ./

CMD ["npm", "run", "dev"]
