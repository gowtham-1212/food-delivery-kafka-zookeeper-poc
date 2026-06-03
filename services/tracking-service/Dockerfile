FROM node:20-slim

WORKDIR /app

COPY services/tracking-service/package.json ./
RUN npm install

COPY services/tracking-service/ ./

EXPOSE 3004

CMD ["npm", "run", "dev"]
