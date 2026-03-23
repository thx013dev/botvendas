# Bot pronto para Railway

## 1) Antes de subir
Gere um token novo no Discord Developer Portal e use esse token novo nas variáveis do Railway.

## 2) Arquivos
- `index.js`: código do bot já adaptado para Railway
- `package.json`: dependências e start script
- `.env.example`: exemplo das variáveis

## 3) Deploy no Railway
1. Crie um repositório no GitHub e envie estes arquivos
2. No Railway, clique em **New Project** > **Deploy from GitHub repo**
3. Selecione o repositório
4. Em **Variables**, crie:
   - `DISCORD_TOKEN`
   - `CLIENT_ID`
   - `GUILD_ID`
   - `DB_PATH=./data/vendas.sqlite`
5. O Railway vai rodar `npm start` automaticamente

## 4) Observação importante
O SQLite funciona, mas em hospedagem sem disco persistente os dados podem ser perdidos ao reiniciar.
