// BOT DE VENDAS PROFISSIONAL - DISCORD.JS V14
// Requisitos:
// npm i discord.js sqlite3 node-cron
// Node.js 18+

const {
  Client,
  GatewayIntentBits,
  Partials,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  SlashCommandBuilder,
  REST,
  Routes,
  PermissionFlagsBits,
  ChannelType,
  MessageFlags,
} = require('discord.js');
const sqlite3 = require('sqlite3').verbose();
const cron = require('node-cron');
const fs = require('fs');
const path = require('path');

// =========================
// CONFIGURAÇÃO
// =========================
const config = {
  token: process.env.DISCORD_TOKEN,
  clientId: process.env.CLIENT_ID,
  guildId: process.env.GUILD_ID,

  branding: {
    nomeSistema: 'Sistema de Vendas — Afiliados',
    footerIcon: null,
    logo: null,
    corPadrao: 0x00d2ff,
    corSucesso: 0x2ecc71,
    corPendente: 0xf1c40f,
    corErro: 0xe74c3c,
    corInfo: 0x5865f2,
  },

  canais: {
    aprovacao: '1484251808607244311',
    fechamento: '1484253548932763670',
    cashback: '1484251684896243834',
    logsAdmin: '1484261980679438469',
    painel: null,
  },

  painelMensagemId: null,

  cargos: {
    staffAprovador: ['1484251067486048256', '1484049287431196841'],
    adminManual: ['1484049287431196841'],
    cashback: {
      membro: '1484049287431196840',
      sub: '1484250926305906850',
      lid: '1484250984447217676',
      aux: '1484251183781646409',
      resp: '1484251067486048256',
    },
  },

  cashbackPorCargo: {
    Membro: 6.0,
    Sub: 7.0,
    Lid: 8.0,
    Aux: 10.0,
    Resp: 12.0,
  },

  fechamento: {
    timezone: 'America/Sao_Paulo',
    cron: '59 23 * * *',
    metaDiaria: 10000,
  },

  seguranca: {
    tempoCooldownSegundos: 15,
  },
};

// =========================
// CLIENT
// =========================
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
  ],
  partials: [Partials.Channel],
});

// =========================
// DATABASE
// =========================
function validateEnv() {
  const required = ['DISCORD_TOKEN', 'CLIENT_ID', 'GUILD_ID'];
  const missing = required.filter((key) => !process.env[key]);

  if (missing.length) {
    throw new Error(`Variáveis ausentes: ${missing.join(', ')}`);
  }
}

const dbPath = process.env.DB_PATH || path.join(process.cwd(), 'data', 'vendas.sqlite');
fs.mkdirSync(path.dirname(dbPath), { recursive: true });
const db = new sqlite3.Database(dbPath);

function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}

function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

async function initDatabase() {
  await dbRun(`
    CREATE TABLE IF NOT EXISTS vendas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      afiliado_id TEXT NOT NULL,
      afiliado_nome TEXT NOT NULL,
      registrador_id TEXT NOT NULL,
      registrador_nome TEXT NOT NULL,
      cliente_id TEXT NOT NULL,
      item TEXT NOT NULL,
      valor REAL NOT NULL,
      cargo_cashback TEXT NOT NULL,
      porcentagem_cashback REAL NOT NULL,
      cashback REAL NOT NULL,
      status TEXT NOT NULL DEFAULT 'pendente',
      criado_em TEXT NOT NULL,
      aprovado_em TEXT,
      aprovado_por_id TEXT,
      aprovado_por_nome TEXT,
      recusado_em TEXT,
      recusado_por_id TEXT,
      recusado_por_nome TEXT,
      motivo_recusa TEXT,
      guild_id TEXT NOT NULL,
      canal_origem_id TEXT,
      mensagem_origem_id TEXT,
      mensagem_aprovacao_id TEXT,
      data_ref TEXT NOT NULL
    )
  `);

  await dbRun(`ALTER TABLE vendas ADD COLUMN mensagem_origem_id TEXT`).catch(() => {});

  await dbRun(`
    CREATE TABLE IF NOT EXISTS cooldowns (
      user_id TEXT PRIMARY KEY,
      ultimo_uso INTEGER NOT NULL
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS configuracoes (
      chave TEXT PRIMARY KEY,
      valor TEXT NOT NULL
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS metas_afiliados (
      guild_id TEXT NOT NULL,
      afiliado_id TEXT NOT NULL,
      meta REAL NOT NULL,
      atualizado_em TEXT NOT NULL,
      PRIMARY KEY (guild_id, afiliado_id)
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS cashback_config (
      guild_id TEXT NOT NULL,
      cargo_nome TEXT NOT NULL,
      porcentagem REAL NOT NULL,
      atualizado_em TEXT NOT NULL,
      PRIMARY KEY (guild_id, cargo_nome)
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS cashback_ajustes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      guild_id TEXT NOT NULL,
      afiliado_id TEXT NOT NULL,
      afiliado_nome TEXT NOT NULL,
      valor REAL NOT NULL,
      tipo TEXT NOT NULL,
      motivo TEXT NOT NULL,
      criado_em TEXT NOT NULL,
      staff_id TEXT NOT NULL,
      staff_nome TEXT NOT NULL
    )
  `);
}

// =========================
// HELPERS
// =========================
function agoraISO() {
  return new Date().toISOString();
}

function dataBrasil(date = new Date()) {
  const d = new Intl.DateTimeFormat('pt-BR', {
    timeZone: config.fechamento.timezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  return d.split('/').reverse().join('-');
}

function dataHoraBrasil(date = new Date()) {
  return new Intl.DateTimeFormat('pt-BR', {
    timeZone: config.fechamento.timezone,
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function formatMoney(value) {
  return Number(value || 0).toLocaleString('pt-BR', {
    style: 'currency',
    currency: 'BRL',
  });
}

function calcCashback(valor, porcentagem) {
  return Number((((Number(valor) || 0) * porcentagem) / 100).toFixed(2));
}

function hasAnyRole(member, roleIds = []) {
  return roleIds.some((r) => member.roles.cache.has(r));
}

function cleanText(str) {
  return String(str || '').trim();
}

function buildFooterText(extra = '') {
  return `${config.branding.nomeSistema}${extra ? ` • ${extra}` : ''}`;
}

function applyBranding(embed, footerText) {
  if (config.branding.logo) embed.setThumbnail(config.branding.logo);
  if (config.branding.footerIcon) {
    embed.setFooter({ text: footerText, iconURL: config.branding.footerIcon });
  } else {
    embed.setFooter({ text: footerText });
  }
  return embed;
}

async function logAdmin(guild, titulo, descricao, cor = 0x5865f2) {
  try {
    const channel = guild.channels.cache.get(config.canais.logsAdmin);
    if (!channel || channel.type !== ChannelType.GuildText) return;
    const embed = new EmbedBuilder()
      .setColor(cor)
      .setTitle(titulo)
      .setDescription(descricao)
      .setTimestamp();
    applyBranding(embed, buildFooterText('Log administrativo'));
    await channel.send({ embeds: [embed] });
  } catch (e) {
    console.error('Erro ao enviar log admin:', e.message);
  }
}

async function checkCooldown(userId) {
  const now = Date.now();
  const row = await dbGet('SELECT * FROM cooldowns WHERE user_id = ?', [userId]);

  if (!row) {
    await dbRun(
      'INSERT OR REPLACE INTO cooldowns (user_id, ultimo_uso) VALUES (?, ?)',
      [userId, now]
    );
    return false;
  }

  const diff = (now - row.ultimo_uso) / 1000;
  if (diff < config.seguranca.tempoCooldownSegundos) {
    return Math.ceil(config.seguranca.tempoCooldownSegundos - diff);
  }

  await dbRun('UPDATE cooldowns SET ultimo_uso = ? WHERE user_id = ?', [now, userId]);
  return false;
}

async function carregarCashbacksGuild(guildId) {
  const rows = await dbAll(
    `SELECT cargo_nome, porcentagem FROM cashback_config WHERE guild_id = ?`,
    [guildId]
  );

  const base = { ...config.cashbackPorCargo };
  for (const row of rows) {
    base[row.cargo_nome] = Number(row.porcentagem);
  }
  return base;
}

async function getCashbackPercent(guildId, cargoNome) {
  const row = await dbGet(
    `SELECT porcentagem FROM cashback_config WHERE guild_id = ? AND cargo_nome = ?`,
    [guildId, cargoNome]
  );
  if (row) return Number(row.porcentagem);
  return config.cashbackPorCargo[cargoNome] ?? 0;
}

async function detectarCargoCashback(member) {
  const ordem = [
    { chave: 'resp', nome: 'Resp' },
    { chave: 'aux', nome: 'Aux' },
    { chave: 'lid', nome: 'Lid' },
    { chave: 'sub', nome: 'Sub' },
    { chave: 'membro', nome: 'Membro' },
  ];

  for (const cargo of ordem) {
    const roleId = config.cargos.cashback[cargo.chave];
    if (roleId && member.roles.cache.has(roleId)) {
      return {
        cargo: cargo.nome,
        porcentagem: await getCashbackPercent(member.guild.id, cargo.nome),
      };
    }
  }

  return null;
}

function getWeekStartEnd() {
  const now = new Date();
  const localeDate = new Date(
    now.toLocaleString('en-US', { timeZone: config.fechamento.timezone })
  );
  const day = localeDate.getDay();
  const diffToMonday = day === 0 ? -6 : 1 - day;
  const start = new Date(localeDate);
  start.setDate(localeDate.getDate() + diffToMonday);
  start.setHours(0, 0, 0, 0);

  const end = new Date(start);
  end.setDate(start.getDate() + 7);

  return { start, end };
}

function getMonthStartEnd() {
  const now = new Date();
  const localeDate = new Date(
    now.toLocaleString('en-US', { timeZone: config.fechamento.timezone })
  );
  const start = new Date(localeDate.getFullYear(), localeDate.getMonth(), 1, 0, 0, 0, 0);
  const end = new Date(localeDate.getFullYear(), localeDate.getMonth() + 1, 1, 0, 0, 0, 0);
  return { start, end };
}

function toIsoLocal(date) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.000Z`;
}

function createPainelAutomaticoButtons() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId('painel_refresh')
        .setLabel('Atualizar Painel')
        .setStyle(ButtonStyle.Primary)
        .setEmoji('🔄')
    ),
  ];
}

function createLimparHistoricoButtons(userId) {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`confirmar_limpar_historico:${userId}`)
        .setLabel('Confirmar limpeza')
        .setStyle(ButtonStyle.Danger)
        .setEmoji('🧹'),
      new ButtonBuilder()
        .setCustomId(`cancelar_limpar_historico:${userId}`)
        .setLabel('Cancelar')
        .setStyle(ButtonStyle.Secondary)
        .setEmoji('❌')
    ),
  ];
}

async function getRanking(guildId, periodo) {
  if (periodo === 'dia') {
    return dbAll(
      `SELECT afiliado_id, afiliado_nome, COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as total_cashback
       FROM vendas
       WHERE guild_id = ? AND data_ref = ? AND status = 'aprovada'
       GROUP BY afiliado_id, afiliado_nome
       ORDER BY total DESC`,
      [guildId, dataBrasil()]
    );
  }

  if (periodo === 'semana') {
    const { start, end } = getWeekStartEnd();
    return dbAll(
      `SELECT afiliado_id, afiliado_nome, COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as total_cashback
       FROM vendas
       WHERE guild_id = ? AND status = 'aprovada' AND criado_em >= ? AND criado_em < ?
       GROUP BY afiliado_id, afiliado_nome
       ORDER BY total DESC`,
      [guildId, toIsoLocal(start), toIsoLocal(end)]
    );
  }

  if (periodo === 'mes') {
    const { start, end } = getMonthStartEnd();
    return dbAll(
      `SELECT afiliado_id, afiliado_nome, COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as total_cashback
       FROM vendas
       WHERE guild_id = ? AND status = 'aprovada' AND criado_em >= ? AND criado_em < ?
       GROUP BY afiliado_id, afiliado_nome
       ORDER BY total DESC`,
      [guildId, toIsoLocal(start), toIsoLocal(end)]
    );
  }

  return dbAll(
    `SELECT afiliado_id, afiliado_nome, COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as total_cashback
     FROM vendas
     WHERE guild_id = ? AND status = 'aprovada'
     GROUP BY afiliado_id, afiliado_nome
     ORDER BY total DESC`,
    [guildId]
  );
}

async function getRelatorioAfiliado(guildId, afiliadoId) {
  const dia = await dbGet(
    `SELECT COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as cashback
     FROM vendas
     WHERE guild_id = ? AND afiliado_id = ? AND status = 'aprovada' AND data_ref = ?`,
    [guildId, afiliadoId, dataBrasil()]
  );

  const { start: s1, end: e1 } = getWeekStartEnd();
  const semana = await dbGet(
    `SELECT COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as cashback
     FROM vendas
     WHERE guild_id = ? AND afiliado_id = ? AND status = 'aprovada' AND criado_em >= ? AND criado_em < ?`,
    [guildId, afiliadoId, toIsoLocal(s1), toIsoLocal(e1)]
  );

  const { start: s2, end: e2 } = getMonthStartEnd();
  const mes = await dbGet(
    `SELECT COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as cashback
     FROM vendas
     WHERE guild_id = ? AND afiliado_id = ? AND status = 'aprovada' AND criado_em >= ? AND criado_em < ?`,
    [guildId, afiliadoId, toIsoLocal(s2), toIsoLocal(e2)]
  );

  const geral = await dbGet(
    `SELECT COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total, COALESCE(SUM(cashback),0) as cashback
     FROM vendas
     WHERE guild_id = ? AND afiliado_id = ? AND status = 'aprovada'`,
    [guildId, afiliadoId]
  );

  return { dia, semana, mes, geral };
}

async function setConfiguracao(chave, valor) {
  await dbRun(
    'INSERT OR REPLACE INTO configuracoes (chave, valor) VALUES (?, ?)',
    [chave, String(valor)]
  );
}

async function getConfiguracao(chave) {
  const row = await dbGet('SELECT valor FROM configuracoes WHERE chave = ?', [chave]);
  return row?.valor ?? null;
}

async function getSaldoCashbackAfiliado(guildId, afiliadoId) {
  const vendas = await dbGet(
    `SELECT COALESCE(SUM(cashback), 0) as total
     FROM vendas
     WHERE guild_id = ? AND afiliado_id = ? AND status = 'aprovada'`,
    [guildId, afiliadoId]
  );

  const ajustes = await dbGet(
    `SELECT
      COALESCE(SUM(CASE WHEN tipo = 'credito' THEN valor ELSE 0 END), 0) as creditos,
      COALESCE(SUM(CASE WHEN tipo = 'debito' THEN valor ELSE 0 END), 0) as debitos
     FROM cashback_ajustes
     WHERE guild_id = ? AND afiliado_id = ?`,
    [guildId, afiliadoId]
  );

  const totalVendas = Number(vendas?.total || 0);
  const totalCreditos = Number(ajustes?.creditos || 0);
  const totalDebitos = Number(ajustes?.debitos || 0);
  const saldo = totalVendas + totalCreditos - totalDebitos;

  return {
    totalVendas,
    totalCreditos,
    totalDebitos,
    saldo,
  };
}

async function getExtratoCashbackAfiliado(guildId, afiliadoId) {
  return dbAll(
    `SELECT valor, tipo, motivo, criado_em, staff_id, staff_nome
     FROM cashback_ajustes
     WHERE guild_id = ? AND afiliado_id = ?
     ORDER BY id DESC
     LIMIT 10`,
    [guildId, afiliadoId]
  );
}

async function gerarPainelEstatisticas(guildId) {
  const hoje = dataBrasil();

  const hojeStats = await dbGet(
    `SELECT
      SUM(CASE WHEN status = 'aprovada' THEN 1 ELSE 0 END) as aprovadas,
      SUM(CASE WHEN status = 'recusada' THEN 1 ELSE 0 END) as recusadas,
      SUM(CASE WHEN status = 'pendente' THEN 1 ELSE 0 END) as pendentes,
      SUM(CASE WHEN status = 'aprovada' THEN valor ELSE 0 END) as total_vendido,
      SUM(CASE WHEN status = 'aprovada' THEN cashback ELSE 0 END) as total_cashback
     FROM vendas
     WHERE guild_id = ? AND data_ref = ?`,
    [guildId, hoje]
  );

  const { start: semanaStart, end: semanaEnd } = getWeekStartEnd();
  const semanaStats = await dbGet(
    `SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'aprovada' THEN valor ELSE 0 END) as total_vendido
     FROM vendas
     WHERE guild_id = ? AND criado_em >= ? AND criado_em < ?`,
    [guildId, toIsoLocal(semanaStart), toIsoLocal(semanaEnd)]
  );

  const top = await dbAll(
    `SELECT afiliado_id, afiliado_nome, SUM(valor) as total
     FROM vendas
     WHERE guild_id = ? AND status = 'aprovada'
     GROUP BY afiliado_id, afiliado_nome
     ORDER BY total DESC
     LIMIT 3`,
    [guildId]
  );

  const topText = top.length
    ? top.map((t, i) => {
        const medal = ['🥇', '🥈', '🥉'][i] || '•';
        return `${medal} <@${t.afiliado_id}> — ${formatMoney(t.total)}`;
      }).join('\n')
    : 'Sem dados ainda.';

  const embed = new EmbedBuilder()
    .setColor(config.branding.corInfo)
    .setTitle('📊 Painel de Estatísticas')
    .setDescription('Resumo atual do sistema de vendas.')
    .addFields(
      {
        name: '📅 Hoje',
        value:
          `⏳ Pendentes: ${hojeStats?.pendentes || 0}\n` +
          `✅ Aprovadas: ${hojeStats?.aprovadas || 0}\n` +
          `❌ Recusadas: ${hojeStats?.recusadas || 0}\n` +
          `💰 Total vendido: ${formatMoney(hojeStats?.total_vendido || 0)}\n` +
          `💸 Cashback: ${formatMoney(hojeStats?.total_cashback || 0)}`,
        inline: false,
      },
      {
        name: '📈 Semana',
        value:
          `📦 Vendas: ${semanaStats?.total || 0}\n` +
          `💰 Total vendido: ${formatMoney(semanaStats?.total_vendido || 0)}`,
        inline: false,
      },
      {
        name: '🏆 Top 3 Afiliados',
        value: topText,
        inline: false,
      }
    )
    .setTimestamp();

  applyBranding(embed, buildFooterText('Painel automático'));
  return embed;
}

async function atualizarPainelAutomatico(guildId) {
  try {
    if (!config.canais.painel || !config.painelMensagemId) return false;

    const canal = await client.channels.fetch(config.canais.painel).catch(() => null);
    if (!canal || canal.type !== ChannelType.GuildText) return false;

    const msg = await canal.messages.fetch(config.painelMensagemId).catch(() => null);
    if (!msg) return false;

    const embed = await gerarPainelEstatisticas(guildId);
    await msg.edit({
      embeds: [embed],
      components: createPainelAutomaticoButtons(),
    });

    return true;
  } catch (e) {
    console.error('Erro ao atualizar painel automático:', e.message);
    return false;
  }
}

// =========================
// EMBEDS
// =========================
function createPainelEmbed() {
  const embed = new EmbedBuilder()
    .setColor(config.branding.corPadrao)
    .setTitle('🧾 Vendas — Registro')
    .setDescription(
      [
        'Use os botões abaixo para **registrar uma venda**.',
        'As vendas só entram no ranking após **aprovação**.',
        '',
        'As vendas manuais são restritas à equipe autorizada.',
      ].join('\n')
    )
    .setTimestamp();

  return applyBranding(embed, buildFooterText());
}

function createPainelButtons() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId('abrir_registro_venda')
      .setLabel('Registrar venda')
      .setStyle(ButtonStyle.Primary)
      .setEmoji('🧾'),
    new ButtonBuilder()
      .setCustomId('abrir_registro_manual')
      .setLabel('Manual (ADM)')
      .setStyle(ButtonStyle.Secondary)
      .setEmoji('➕')
  );
}

function buildVendaPendenteEmbed(venda) {
  const embed = new EmbedBuilder()
    .setColor(config.branding.corPendente)
    .setTitle(`🟡 Venda Pendente — ${config.branding.nomeSistema}`)
    .addFields(
      { name: '📎 Afiliado', value: `<@${venda.afiliado_id}>\n${venda.afiliado_nome}`, inline: true },
      { name: '🆔 ID do Cliente', value: String(venda.cliente_id || 'Não informado'), inline: true },
      { name: '📦 Itens', value: venda.item || 'Não informado', inline: false },
      { name: '💰 Valor', value: formatMoney(venda.valor), inline: true },
      { name: '🏷️ Cargo (cashback)', value: `${venda.cargo_cashback} (${venda.porcentagem_cashback}%)`, inline: true },
      { name: '🕒 Status', value: 'Aguardando aprovação', inline: true }
    )
    .setTimestamp();

  return applyBranding(embed, `${buildFooterText()} • ${dataHoraBrasil(new Date(venda.criado_em))}`);
}

function buildVendaAprovadaEmbed(venda) {
  const embed = new EmbedBuilder()
    .setColor(config.branding.corSucesso)
    .setTitle(`✅ Venda Confirmada — ${config.branding.nomeSistema}`)
    .addFields(
      { name: '📎 Afiliado', value: `<@${venda.afiliado_id}>\n${venda.afiliado_nome}`, inline: true },
      { name: '🆔 ID do Cliente', value: String(venda.cliente_id || 'Não informado'), inline: true },
      { name: '📦 Itens', value: venda.item || 'Não informado', inline: false },
      { name: '💰 Valor', value: formatMoney(venda.valor), inline: true },
      { name: '✅ Confirmado por', value: `<@${venda.aprovado_por_id}>\n${venda.aprovado_por_nome}`, inline: true },
      { name: '🏷️ Cargo (cashback)', value: `${venda.cargo_cashback}`, inline: true },
      { name: '📊 % Cashback', value: `${Number(venda.porcentagem_cashback).toFixed(2)}%`, inline: true },
      { name: '🎉 Cashback', value: formatMoney(venda.cashback), inline: true }
    )
    .setTimestamp();

  return applyBranding(embed, `${buildFooterText()} • ${dataHoraBrasil(new Date(venda.aprovado_em || venda.criado_em))}`);
}

function buildVendaRecusadaEmbed(venda) {
  const embed = new EmbedBuilder()
    .setColor(config.branding.corErro)
    .setTitle(`❌ Venda Recusada — ${config.branding.nomeSistema}`)
    .addFields(
      { name: '📎 Afiliado', value: `<@${venda.afiliado_id}>\n${venda.afiliado_nome}`, inline: true },
      { name: '🆔 ID do Cliente', value: String(venda.cliente_id || 'Não informado'), inline: true },
      { name: '📦 Itens', value: venda.item || 'Não informado', inline: false },
      { name: '💰 Valor', value: formatMoney(venda.valor), inline: true },
      { name: '🚫 Recusado por', value: `<@${venda.recusado_por_id}>\n${venda.recusado_por_nome}`, inline: true },
      { name: '📝 Motivo', value: venda.motivo_recusa || 'Não informado', inline: false }
    )
    .setTimestamp();

  return applyBranding(embed, `${buildFooterText()} • ${dataHoraBrasil(new Date(venda.recusado_em || venda.criado_em))}`);
}

function createAprovacaoButtons(vendaId) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`aprovar_venda:${vendaId}`)
      .setLabel('Aprovar')
      .setStyle(ButtonStyle.Success)
      .setEmoji('✅'),
    new ButtonBuilder()
      .setCustomId(`recusar_venda:${vendaId}`)
      .setLabel('Recusar')
      .setStyle(ButtonStyle.Danger)
      .setEmoji('❌')
  );
}

function createDisabledAprovacaoButtons(status) {
  const label = status === 'aprovada' ? 'Venda aprovada' : 'Venda recusada';
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`disabled_a_${status}`)
      .setLabel(label)
      .setStyle(status === 'aprovada' ? ButtonStyle.Success : ButtonStyle.Danger)
      .setDisabled(true)
  );
}

// =========================
// CANAIS ESPECIAIS
// =========================
async function enviarCashbackChannel(guild, venda) {
  try {
    const channel = guild.channels.cache.get(config.canais.cashback);
    if (!channel || channel.type !== ChannelType.GuildText) return;

    await channel.send(
      `💰 **Cashback contabilizado para** <@${venda.afiliado_id}>: ${formatMoney(venda.cashback)} (${Number(venda.porcentagem_cashback).toFixed(2)}% de ${formatMoney(venda.valor)})`
    );
  } catch (e) {
    console.error('Erro ao enviar cashback:', e.message);
  }
}

async function enviarFechamentoDiario(guild) {
  const hoje = dataBrasil();
  const canal = guild.channels.cache.get(config.canais.fechamento);
  if (!canal) return;

  const resumo = await dbGet(
    `SELECT COUNT(*) as total_vendas, COALESCE(SUM(valor), 0) as total_vendido
     FROM vendas
     WHERE guild_id = ? AND data_ref = ? AND status = 'aprovada'`,
    [guild.id, hoje]
  );

  const top5 = await dbAll(
    `SELECT afiliado_id, afiliado_nome, COUNT(*) as quantidade, COALESCE(SUM(valor),0) as total
     FROM vendas
     WHERE guild_id = ? AND data_ref = ? AND status = 'aprovada'
     GROUP BY afiliado_id, afiliado_nome
     ORDER BY total DESC
     LIMIT 5`,
    [guild.id, hoje]
  );

  const percentualMeta = config.fechamento.metaDiaria > 0
    ? ((Number(resumo.total_vendido) / config.fechamento.metaDiaria) * 100)
    : 0;

  const rankingTexto = top5.length
    ? top5.map((u, i) => `${i + 1}) <@${u.afiliado_id}> — ${formatMoney(u.total)} (${u.quantidade})`).join('\n')
    : 'Nenhuma venda aprovada hoje.';

  const embed = new EmbedBuilder()
    .setColor(config.branding.corInfo)
    .setTitle(`🧾 Fechamento do dia — ${new Intl.DateTimeFormat('pt-BR', {
      timeZone: config.fechamento.timezone,
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    }).format(new Date())}`)
    .setDescription('Resumo de vendas do dia (00:00 → 23:59).')
    .addFields(
      { name: '📊 Vendas', value: String(resumo.total_vendas || 0), inline: true },
      { name: '💰 Total vendido', value: formatMoney(resumo.total_vendido || 0), inline: true },
      {
        name: '🎯 Meta do dia',
        value: `${formatMoney(config.fechamento.metaDiaria)} (${percentualMeta.toFixed(0)}%)`,
        inline: true,
      },
      { name: '🏅 Top 5 do dia', value: rankingTexto, inline: false }
    )
    .setTimestamp();

  applyBranding(embed, buildFooterText('Fechamento automático'));

  await canal.send({ embeds: [embed] });
  await atualizarPainelAutomatico(guild.id);
  await logAdmin(guild, '📘 Fechamento automático', 'Fechamento diário executado automaticamente.');
}

// =========================
// SLASH COMMANDS
// =========================
const commands = [
  new SlashCommandBuilder()
    .setName('vendas')
    .setDescription('Envia o painel de registro de vendas.')
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

  new SlashCommandBuilder()
    .setName('painel')
    .setDescription('Mostra o painel de estatísticas.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('painelautomatico')
    .setDescription('Cria ou redefine o painel automático neste canal.')
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

  new SlashCommandBuilder()
    .setName('atualizarpainel')
    .setDescription('Atualiza manualmente o painel automático.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('ranking')
    .setDescription('Mostra ranking de vendas.')
    .addStringOption((opt) =>
      opt.setName('periodo')
        .setDescription('dia, semana, mes, geral')
        .setRequired(true)
        .addChoices(
          { name: 'Dia', value: 'dia' },
          { name: 'Semana', value: 'semana' },
          { name: 'Mês', value: 'mes' },
          { name: 'Geral', value: 'geral' }
        )
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('relatorio')
    .setDescription('Mostra relatório de um afiliado.')
    .addUserOption((opt) =>
      opt.setName('usuario').setDescription('Afiliado').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('setmeta')
    .setDescription('Define a meta diária geral.')
    .addNumberOption((opt) =>
      opt.setName('valor').setDescription('Novo valor da meta diária').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

  new SlashCommandBuilder()
    .setName('setcashback')
    .setDescription('Adiciona ou altera cashback de um cargo.')
    .addStringOption((opt) =>
      opt.setName('cargo').setDescription('Nome do cargo cashback').setRequired(true)
    )
    .addNumberOption((opt) =>
      opt.setName('porcentagem').setDescription('Porcentagem').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

  new SlashCommandBuilder()
    .setName('vercashbacks')
    .setDescription('Mostra os cashbacks atuais.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('fechamento')
    .setDescription('Gera manualmente o fechamento do dia.')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('descontarcashback')
    .setDescription('Desconta valor do cashback de um afiliado.')
    .addUserOption((opt) =>
      opt.setName('usuario').setDescription('Afiliado').setRequired(true)
    )
    .addNumberOption((opt) =>
      opt.setName('valor').setDescription('Valor a descontar').setRequired(true)
    )
    .addStringOption((opt) =>
      opt.setName('motivo').setDescription('Motivo do desconto').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('adicionarcashback')
    .setDescription('Adiciona valor manual ao cashback de um afiliado.')
    .addUserOption((opt) =>
      opt.setName('usuario').setDescription('Afiliado').setRequired(true)
    )
    .addNumberOption((opt) =>
      opt.setName('valor').setDescription('Valor a adicionar').setRequired(true)
    )
    .addStringOption((opt) =>
      opt.setName('motivo').setDescription('Motivo do crédito').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('saldoafiliado')
    .setDescription('Mostra o saldo de cashback de um afiliado.')
    .addUserOption((opt) =>
      opt.setName('usuario').setDescription('Afiliado').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('extratocashback')
    .setDescription('Mostra o extrato de cashback de um afiliado.')
    .addUserOption((opt) =>
      opt.setName('usuario').setDescription('Afiliado').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

  new SlashCommandBuilder()
    .setName('limparhistorico')
    .setDescription('Apaga totalmente o histórico de um afiliado com confirmação.')
    .addUserOption((opt) =>
      opt.setName('usuario').setDescription('Afiliado').setRequired(true)
    )
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),
].map((cmd) => cmd.toJSON());

async function registrarSlashCommands() {
  const rest = new REST({ version: '10' }).setToken(config.token);
  await rest.put(
    Routes.applicationGuildCommands(config.clientId, config.guildId),
    { body: commands }
  );
  console.log('Slash commands registrados com sucesso.');
}

// =========================
// INTERACTIONS
// =========================
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      if (interaction.commandName === 'vendas') {
        await interaction.reply({
          content: 'Painel enviado com sucesso.',
          flags: MessageFlags.Ephemeral,
        });

        const channel = interaction.channel;
        if (!channel || channel.type !== ChannelType.GuildText) {
          await interaction.followUp({
            content: 'Esse comando só pode ser usado em canal de texto normal.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        await channel.send({
          embeds: [createPainelEmbed()],
          components: [createPainelButtons()],
        });
        return;
      }

      if (interaction.commandName === 'painel') {
        const embed = await gerarPainelEstatisticas(interaction.guild.id);
        await interaction.reply({
          embeds: [embed],
          components: createPainelAutomaticoButtons(),
        });
        return;
      }

      if (interaction.commandName === 'painelautomatico') {
        const channel = interaction.channel;
        if (!channel || channel.type !== ChannelType.GuildText) {
          await interaction.reply({
            content: 'Esse comando só pode ser usado em canal de texto normal.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        config.canais.painel = channel.id;
        await setConfiguracao('painel_canal_id', channel.id);

        if (config.painelMensagemId) {
          try {
            const oldMsg = await channel.messages.fetch(config.painelMensagemId).catch(() => null);
            if (oldMsg) {
              await oldMsg.delete().catch(() => null);
            }
          } catch (_) {}
        }

        const embed = await gerarPainelEstatisticas(interaction.guild.id);
        const msg = await channel.send({
          embeds: [embed],
          components: createPainelAutomaticoButtons(),
        });

        config.painelMensagemId = msg.id;
        await setConfiguracao('painel_mensagem_id', msg.id);

        await interaction.reply({
          content: '✅ Painel automático criado neste canal.',
          flags: MessageFlags.Ephemeral,
        });

        await logAdmin(
          interaction.guild,
          '📊 Painel automático configurado',
          `${interaction.user.tag} configurou o painel automático em <#${channel.id}>.`
        );
        return;
      }

      if (interaction.commandName === 'atualizarpainel') {
        if (!config.canais.painel || !config.painelMensagemId) {
          await interaction.reply({
            content: 'Painel automático não está configurado.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const ok = await atualizarPainelAutomatico(interaction.guild.id);
        await interaction.reply({
          content: ok ? '✅ Painel atualizado com sucesso.' : '❌ Não foi possível atualizar o painel.',
          flags: MessageFlags.Ephemeral,
        });
        return;
      }

      if (interaction.commandName === 'ranking') {
        const periodo = interaction.options.getString('periodo', true);
        const ranking = await getRanking(interaction.guild.id, periodo);

        const texto = ranking.length
          ? ranking
              .slice(0, 10)
              .map((u, i) =>
                `${i + 1}) <@${u.afiliado_id}> — ${formatMoney(u.total)} | ${u.quantidade} venda(s) | cashback ${formatMoney(u.total_cashback)}`
              )
              .join('\n')
          : 'Nenhuma venda aprovada nesse período.';

        const nomes = {
          dia: 'do dia',
          semana: 'da semana',
          mes: 'do mês',
          geral: 'geral',
        };

        const embed = new EmbedBuilder()
          .setColor(config.branding.corInfo)
          .setTitle(`🏆 Ranking ${nomes[periodo]}`)
          .setDescription(texto)
          .setTimestamp();

        applyBranding(embed, buildFooterText());
        await interaction.reply({ embeds: [embed] });
        return;
      }

      if (interaction.commandName === 'relatorio') {
        const usuario = interaction.options.getUser('usuario', true);
        const rel = await getRelatorioAfiliado(interaction.guild.id, usuario.id);

        const embed = new EmbedBuilder()
          .setColor(config.branding.corInfo)
          .setTitle(`📊 Relatório de ${usuario.username}`)
          .addFields(
            {
              name: 'Hoje',
              value: `Vendas: ${rel.dia.quantidade || 0}\nTotal: ${formatMoney(rel.dia.total)}\nCashback: ${formatMoney(rel.dia.cashback)}`,
              inline: true,
            },
            {
              name: 'Semana',
              value: `Vendas: ${rel.semana.quantidade || 0}\nTotal: ${formatMoney(rel.semana.total)}\nCashback: ${formatMoney(rel.semana.cashback)}`,
              inline: true,
            },
            {
              name: 'Mês',
              value: `Vendas: ${rel.mes.quantidade || 0}\nTotal: ${formatMoney(rel.mes.total)}\nCashback: ${formatMoney(rel.mes.cashback)}`,
              inline: true,
            },
            {
              name: 'Geral',
              value: `Vendas: ${rel.geral.quantidade || 0}\nTotal: ${formatMoney(rel.geral.total)}\nCashback: ${formatMoney(rel.geral.cashback)}`,
              inline: true,
            }
          )
          .setTimestamp();

        applyBranding(embed, buildFooterText('Relatório por afiliado'));
        await interaction.reply({ embeds: [embed] });
        return;
      }

      if (interaction.commandName === 'setmeta') {
        const valor = interaction.options.getNumber('valor', true);
        config.fechamento.metaDiaria = valor;
        await dbRun(
          'INSERT OR REPLACE INTO configuracoes (chave, valor) VALUES (?, ?)',
          ['meta_diaria', String(valor)]
        );
        await interaction.reply({
          content: `Meta diária atualizada para ${formatMoney(valor)}.`,
          flags: MessageFlags.Ephemeral,
        });
        await logAdmin(interaction.guild, '🎯 Meta geral atualizada', `${interaction.user.tag} definiu a meta geral em ${formatMoney(valor)}.`);
        await atualizarPainelAutomatico(interaction.guild.id);
        return;
      }

      if (interaction.commandName === 'setcashback') {
        const cargo = cleanText(interaction.options.getString('cargo', true));
        const porcentagem = interaction.options.getNumber('porcentagem', true);

        await dbRun(
          `INSERT OR REPLACE INTO cashback_config (guild_id, cargo_nome, porcentagem, atualizado_em)
           VALUES (?, ?, ?, ?)`,
          [interaction.guild.id, cargo, porcentagem, agoraISO()]
        );

        await interaction.reply({
          content: `Cashback de **${cargo}** definido em **${porcentagem}%**.`,
          flags: MessageFlags.Ephemeral,
        });

        await logAdmin(
          interaction.guild,
          '💸 Cashback atualizado',
          `${interaction.user.tag} definiu ${cargo} = ${porcentagem}%.`
        );
        return;
      }

      if (interaction.commandName === 'vercashbacks') {
        const map = await carregarCashbacksGuild(interaction.guild.id);
        const texto = Object.entries(map)
          .map(([cargo, perc]) => `• ${cargo}: ${perc}%`)
          .join('\n');

        const embed = new EmbedBuilder()
          .setColor(config.branding.corInfo)
          .setTitle('💸 Cashbacks atuais')
          .setDescription(texto || 'Nenhum cashback configurado.')
          .setTimestamp();

        applyBranding(embed, buildFooterText('Configuração de cashback'));
        await interaction.reply({ embeds: [embed], flags: MessageFlags.Ephemeral });
        return;
      }

      if (interaction.commandName === 'fechamento') {
        await enviarFechamentoDiario(interaction.guild);
        await interaction.reply({ content: 'Fechamento enviado com sucesso.', flags: MessageFlags.Ephemeral });
        await logAdmin(interaction.guild, '📘 Fechamento manual', `${interaction.user.tag} executou o fechamento manual.`);
        return;
      }

      if (interaction.commandName === 'descontarcashback') {
        const usuario = interaction.options.getUser('usuario', true);
        const valor = interaction.options.getNumber('valor', true);
        const motivo = cleanText(interaction.options.getString('motivo', true));

        if (valor <= 0) {
          await interaction.reply({
            content: 'Informe um valor maior que zero.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const saldoAtual = await getSaldoCashbackAfiliado(interaction.guild.id, usuario.id);

        await dbRun(
          `INSERT INTO cashback_ajustes (
            guild_id, afiliado_id, afiliado_nome, valor, tipo, motivo, criado_em, staff_id, staff_nome
          ) VALUES (?, ?, ?, ?, 'debito', ?, ?, ?, ?)`,
          [
            interaction.guild.id,
            usuario.id,
            usuario.tag,
            valor,
            motivo,
            agoraISO(),
            interaction.user.id,
            interaction.user.tag,
          ]
        );

        const saldoNovo = await getSaldoCashbackAfiliado(interaction.guild.id, usuario.id);

        const embed = new EmbedBuilder()
          .setColor(config.branding.corErro)
          .setTitle('💸 Desconto de cashback realizado')
          .addFields(
            { name: 'Afiliado', value: `<@${usuario.id}>`, inline: true },
            { name: 'Valor descontado', value: formatMoney(valor), inline: true },
            { name: 'Motivo', value: motivo, inline: false },
            { name: 'Saldo anterior', value: formatMoney(saldoAtual.saldo), inline: true },
            { name: 'Saldo atual', value: formatMoney(saldoNovo.saldo), inline: true },
          )
          .setTimestamp();

        applyBranding(embed, buildFooterText('Desconto de cashback'));

        await interaction.reply({
          embeds: [embed],
          flags: MessageFlags.Ephemeral,
        });

        await logAdmin(
          interaction.guild,
          '💸 Desconto de cashback',
          `${interaction.user.tag} descontou ${formatMoney(valor)} de <@${usuario.id}>. Motivo: ${motivo}`,
          0xe74c3c
        );

        return;
      }

      if (interaction.commandName === 'adicionarcashback') {
        const usuario = interaction.options.getUser('usuario', true);
        const valor = interaction.options.getNumber('valor', true);
        const motivo = cleanText(interaction.options.getString('motivo', true));

        if (valor <= 0) {
          await interaction.reply({
            content: 'Informe um valor maior que zero.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const saldoAtual = await getSaldoCashbackAfiliado(interaction.guild.id, usuario.id);

        await dbRun(
          `INSERT INTO cashback_ajustes (
            guild_id, afiliado_id, afiliado_nome, valor, tipo, motivo, criado_em, staff_id, staff_nome
          ) VALUES (?, ?, ?, ?, 'credito', ?, ?, ?, ?)`,
          [
            interaction.guild.id,
            usuario.id,
            usuario.tag,
            valor,
            motivo,
            agoraISO(),
            interaction.user.id,
            interaction.user.tag,
          ]
        );

        const saldoNovo = await getSaldoCashbackAfiliado(interaction.guild.id, usuario.id);

        const embed = new EmbedBuilder()
          .setColor(config.branding.corSucesso)
          .setTitle('💰 Crédito de cashback realizado')
          .addFields(
            { name: 'Afiliado', value: `<@${usuario.id}>`, inline: true },
            { name: 'Valor adicionado', value: formatMoney(valor), inline: true },
            { name: 'Motivo', value: motivo, inline: false },
            { name: 'Saldo anterior', value: formatMoney(saldoAtual.saldo), inline: true },
            { name: 'Saldo atual', value: formatMoney(saldoNovo.saldo), inline: true },
          )
          .setTimestamp();

        applyBranding(embed, buildFooterText('Crédito de cashback'));

        await interaction.reply({
          embeds: [embed],
          flags: MessageFlags.Ephemeral,
        });

        await logAdmin(
          interaction.guild,
          '💰 Crédito de cashback',
          `${interaction.user.tag} adicionou ${formatMoney(valor)} para <@${usuario.id}>. Motivo: ${motivo}`,
          0x2ecc71
        );

        return;
      }

      if (interaction.commandName === 'saldoafiliado') {
        const usuario = interaction.options.getUser('usuario', true);
        const saldo = await getSaldoCashbackAfiliado(interaction.guild.id, usuario.id);

        const embed = new EmbedBuilder()
          .setColor(config.branding.corInfo)
          .setTitle(`📊 Saldo de cashback — ${usuario.username}`)
          .addFields(
            { name: 'Cashback por vendas aprovadas', value: formatMoney(saldo.totalVendas), inline: false },
            { name: 'Créditos manuais', value: formatMoney(saldo.totalCreditos), inline: true },
            { name: 'Débitos manuais', value: formatMoney(saldo.totalDebitos), inline: true },
            { name: 'Saldo atual', value: formatMoney(saldo.saldo), inline: false },
          )
          .setTimestamp();

        applyBranding(embed, buildFooterText('Saldo de cashback'));

        await interaction.reply({
          embeds: [embed],
          flags: MessageFlags.Ephemeral,
        });

        return;
      }

      if (interaction.commandName === 'extratocashback') {
        const usuario = interaction.options.getUser('usuario', true);
        const extrato = await getExtratoCashbackAfiliado(interaction.guild.id, usuario.id);
        const saldo = await getSaldoCashbackAfiliado(interaction.guild.id, usuario.id);

        const texto = extrato.length
          ? extrato.map((item, i) => {
              const emoji = item.tipo === 'credito' ? '🟢' : '🔴';
              const sinal = item.tipo === 'credito' ? '+' : '-';
              return `${i + 1}. ${emoji} ${sinal}${formatMoney(item.valor)}\nMotivo: ${item.motivo}\nPor: ${item.staff_nome}\nData: ${dataHoraBrasil(new Date(item.criado_em))}`;
            }).join('\n\n')
          : 'Nenhum ajuste manual encontrado.';

        const embed = new EmbedBuilder()
          .setColor(config.branding.corInfo)
          .setTitle(`📜 Extrato de cashback — ${usuario.username}`)
          .setDescription(texto)
          .addFields({
            name: 'Saldo atual',
            value: formatMoney(saldo.saldo),
            inline: false,
          })
          .setTimestamp();

        applyBranding(embed, buildFooterText('Extrato de cashback'));

        await interaction.reply({
          embeds: [embed],
          flags: MessageFlags.Ephemeral,
        });

        return;
      }

      if (interaction.commandName === 'limparhistorico') {
        const usuario = interaction.options.getUser('usuario', true);

        const vendasAntes = await dbGet(
          `SELECT COUNT(*) as total FROM vendas WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, usuario.id]
        );

        const ajustesAntes = await dbGet(
          `SELECT COUNT(*) as total FROM cashback_ajustes WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, usuario.id]
        );

        const metaAntes = await dbGet(
          `SELECT meta FROM metas_afiliados WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, usuario.id]
        );

        const embed = new EmbedBuilder()
          .setColor(config.branding.corErro)
          .setTitle('⚠️ Confirmação de limpeza')
          .setDescription(`Você está prestes a apagar **todo o histórico** de <@${usuario.id}>.`)
          .addFields(
            { name: 'Vendas encontradas', value: String(vendasAntes?.total || 0), inline: true },
            { name: 'Ajustes encontrados', value: String(ajustesAntes?.total || 0), inline: true },
            { name: 'Meta individual', value: metaAntes ? 'Existe' : 'Não existe', inline: true },
            {
              name: 'Atenção',
              value: 'Essa ação remove permanentemente os dados do usuário neste servidor.',
              inline: false,
            }
          )
          .setTimestamp();

        applyBranding(embed, buildFooterText('Confirmação de limpeza'));

        await interaction.reply({
          embeds: [embed],
          components: createLimparHistoricoButtons(usuario.id),
          flags: MessageFlags.Ephemeral,
        });

        return;
      }
    }

    if (interaction.isButton()) {
      if (interaction.customId.startsWith('confirmar_limpar_historico:')) {
        const userId = interaction.customId.split(':')[1];

        const vendasAntes = await dbGet(
          `SELECT COUNT(*) as total FROM vendas WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, userId]
        );

        const ajustesAntes = await dbGet(
          `SELECT COUNT(*) as total FROM cashback_ajustes WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, userId]
        );

        const metaAntes = await dbGet(
          `SELECT meta FROM metas_afiliados WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, userId]
        );

        await dbRun(
          `DELETE FROM vendas WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, userId]
        );

        await dbRun(
          `DELETE FROM cashback_ajustes WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, userId]
        );

        await dbRun(
          `DELETE FROM metas_afiliados WHERE guild_id = ? AND afiliado_id = ?`,
          [interaction.guild.id, userId]
        );

        const embed = new EmbedBuilder()
          .setColor(config.branding.corErro)
          .setTitle('🧹 Histórico apagado com sucesso')
          .setDescription(`O histórico de <@${userId}> foi removido.`)
          .addFields(
            { name: 'Vendas removidas', value: String(vendasAntes?.total || 0), inline: true },
            { name: 'Ajustes removidos', value: String(ajustesAntes?.total || 0), inline: true },
            { name: 'Meta removida', value: metaAntes ? 'Sim' : 'Não', inline: true }
          )
          .setTimestamp();

        applyBranding(embed, buildFooterText('Histórico removido'));

        await interaction.update({
          embeds: [embed],
          components: [],
        });

        await atualizarPainelAutomatico(interaction.guild.id);

        await logAdmin(
          interaction.guild,
          '🧹 Histórico apagado',
          `${interaction.user.tag} apagou totalmente o histórico de <@${userId}>.`,
          0xe74c3c
        );

        return;
      }

      if (interaction.customId.startsWith('cancelar_limpar_historico:')) {
        const userId = interaction.customId.split(':')[1];

        const embed = new EmbedBuilder()
          .setColor(config.branding.corInfo)
          .setTitle('❌ Limpeza cancelada')
          .setDescription(`A limpeza do histórico de <@${userId}> foi cancelada.`)
          .setTimestamp();

        applyBranding(embed, buildFooterText('Ação cancelada'));

        await interaction.update({
          embeds: [embed],
          components: [],
        });

        return;
      }

      if (interaction.customId === 'painel_refresh') {
        const embed = await gerarPainelEstatisticas(interaction.guild.id);

        await interaction.update({
          embeds: [embed],
          components: createPainelAutomaticoButtons(),
        });

        return;
      }

      if (
        interaction.customId === 'abrir_registro_venda' ||
        interaction.customId === 'abrir_registro_manual'
      ) {
        const isManual = interaction.customId === 'abrir_registro_manual';

        if (isManual && !hasAnyRole(interaction.member, config.cargos.adminManual)) {
          await interaction.reply({
            content: 'Você não tem permissão para registrar vendas manuais.',
            flags: MessageFlags.Ephemeral,
          });
          await logAdmin(interaction.guild, '🚫 Tentativa negada', `${interaction.user.tag} tentou usar registro manual sem permissão.`, 0xe74c3c);
          return;
        }

        const cooldown = await checkCooldown(interaction.user.id);
        if (cooldown && !isManual) {
          await interaction.reply({
            content: `Aguarde ${cooldown}s para registrar outra venda.`,
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const modal = new ModalBuilder()
          .setCustomId(isManual ? 'modal_registro_manual' : 'modal_registro_venda')
          .setTitle(isManual ? 'Registro Manual de Venda' : 'Registrar venda');

        const clienteInput = new TextInputBuilder()
          .setCustomId('cliente_input')
          .setLabel('ID do cliente')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setPlaceholder('Ex: 744');

        const itemInput = new TextInputBuilder()
          .setCustomId('item_input')
          .setLabel('Itens vendidos')
          .setStyle(TextInputStyle.Paragraph)
          .setRequired(true)
          .setPlaceholder('Ex: Carro voador OVNI');

        const valorInput = new TextInputBuilder()
          .setCustomId('valor_input')
          .setLabel('Valor da venda')
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setPlaceholder('Ex: 200,00');

        modal.addComponents(
          new ActionRowBuilder().addComponents(clienteInput),
          new ActionRowBuilder().addComponents(itemInput),
          new ActionRowBuilder().addComponents(valorInput)
        );

        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId.startsWith('aprovar_venda:')) {
        if (!hasAnyRole(interaction.member, config.cargos.staffAprovador)) {
          await interaction.reply({
            content: 'Você não tem permissão para aprovar vendas.',
            flags: MessageFlags.Ephemeral,
          });
          await logAdmin(interaction.guild, '🚫 Aprovação negada', `${interaction.user.tag} tentou aprovar venda sem permissão.`, 0xe74c3c);
          return;
        }

        const vendaId = Number(interaction.customId.split(':')[1]);
        const venda = await dbGet(
          'SELECT * FROM vendas WHERE id = ? AND guild_id = ?',
          [vendaId, interaction.guild.id]
        );

        if (!venda) {
          await interaction.reply({ content: 'Venda não encontrada.', flags: MessageFlags.Ephemeral });
          return;
        }

        if (venda.status !== 'pendente') {
          await interaction.reply({
            content: `Essa venda já está como ${venda.status}.`,
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const aprovadoEm = agoraISO();
        await dbRun(
          `UPDATE vendas
           SET status = 'aprovada', aprovado_em = ?, aprovado_por_id = ?, aprovado_por_nome = ?
           WHERE id = ?`,
          [aprovadoEm, interaction.user.id, interaction.user.tag, vendaId]
        );

        const vendaAtualizada = await dbGet('SELECT * FROM vendas WHERE id = ?', [vendaId]);

        await interaction.update({
          embeds: [buildVendaAprovadaEmbed(vendaAtualizada)],
          components: [createDisabledAprovacaoButtons('aprovada')],
        });

        if (vendaAtualizada.mensagem_origem_id && vendaAtualizada.canal_origem_id) {
          try {
            const canalOrigem = interaction.guild.channels.cache.get(vendaAtualizada.canal_origem_id);
            if (canalOrigem && canalOrigem.type === ChannelType.GuildText) {
              const msgOrigem = await canalOrigem.messages.fetch(vendaAtualizada.mensagem_origem_id);
              await msgOrigem.edit({
                content: `✅ Venda #${vendaId} aprovada por ${interaction.user.tag}.`,
                embeds: [buildVendaAprovadaEmbed(vendaAtualizada)],
              });
            }
          } catch (e) {
            console.error('Erro ao editar mensagem original da aprovação:', e.message);
          }
        }

        await enviarCashbackChannel(interaction.guild, vendaAtualizada);
        await atualizarPainelAutomatico(interaction.guild.id);
        await logAdmin(
          interaction.guild,
          '✅ Venda aprovada',
          `${interaction.user.tag} aprovou a venda #${vendaId} de <@${vendaAtualizada.afiliado_id}> no valor de ${formatMoney(vendaAtualizada.valor)}.`
        );
        return;
      }

      if (interaction.customId.startsWith('recusar_venda:')) {
        if (!hasAnyRole(interaction.member, config.cargos.staffAprovador)) {
          await interaction.reply({
            content: 'Você não tem permissão para recusar vendas.',
            flags: MessageFlags.Ephemeral,
          });
          await logAdmin(interaction.guild, '🚫 Recusa negada', `${interaction.user.tag} tentou recusar venda sem permissão.`, 0xe74c3c);
          return;
        }

        const vendaId = Number(interaction.customId.split(':')[1]);
        const venda = await dbGet(
          'SELECT * FROM vendas WHERE id = ? AND guild_id = ?',
          [vendaId, interaction.guild.id]
        );

        if (!venda) {
          await interaction.reply({ content: 'Venda não encontrada.', flags: MessageFlags.Ephemeral });
          return;
        }

        if (venda.status !== 'pendente') {
          await interaction.reply({
            content: `Essa venda já está como ${venda.status}.`,
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const modal = new ModalBuilder()
          .setCustomId(`modal_recusar_venda:${vendaId}`)
          .setTitle(`Recusar venda #${vendaId}`);

        const motivoInput = new TextInputBuilder()
          .setCustomId('motivo_recusa_input')
          .setLabel('Motivo da recusa')
          .setStyle(TextInputStyle.Paragraph)
          .setRequired(true)
          .setPlaceholder('Explique o motivo para a recusa.');

        modal.addComponents(new ActionRowBuilder().addComponents(motivoInput));
        await interaction.showModal(modal);
        return;
      }
    }

    if (interaction.isModalSubmit()) {
      if (
        interaction.customId === 'modal_registro_venda' ||
        interaction.customId === 'modal_registro_manual'
      ) {
        const clienteId = cleanText(interaction.fields.getTextInputValue('cliente_input'));
        const item = cleanText(interaction.fields.getTextInputValue('item_input'));
        const valorRaw = cleanText(interaction.fields.getTextInputValue('valor_input'));

        const afiliadoId = interaction.user.id;
        const afiliadoNome = interaction.user.tag;
        const afiliadoMember = interaction.member;
        const valor = Number(valorRaw.replace(/\./g, '').replace(',', '.')) || 0;

        const cashbackInfo = await detectarCargoCashback(afiliadoMember);
        if (!cashbackInfo) {
          await interaction.reply({
            content: 'Você não possui nenhum cargo de cashback configurado.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const criadoEm = agoraISO();
        const cashback = calcCashback(valor, cashbackInfo.porcentagem);

        const result = await dbRun(
          `INSERT INTO vendas (
            afiliado_id, afiliado_nome, registrador_id, registrador_nome, cliente_id, item, valor,
            cargo_cashback, porcentagem_cashback, cashback, status, criado_em, guild_id, canal_origem_id, data_ref
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pendente', ?, ?, ?, ?)`,
          [
            afiliadoId,
            afiliadoNome,
            interaction.user.id,
            interaction.user.tag,
            clienteId,
            item,
            valor,
            cashbackInfo.cargo,
            cashbackInfo.porcentagem,
            cashback,
            criadoEm,
            interaction.guild.id,
            interaction.channelId,
            dataBrasil(),
          ]
        );

        const vendaId = result.lastID;
        const venda = await dbGet('SELECT * FROM vendas WHERE id = ?', [vendaId]);

        const canalOrigem = interaction.channel;
        if (!canalOrigem || canalOrigem.type !== ChannelType.GuildText) {
          await interaction.reply({
            content: 'Canal de origem inválido.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const mensagemOrigem = await canalOrigem.send({
          embeds: [buildVendaPendenteEmbed(venda)],
        });

        const aprovacaoChannel = interaction.guild.channels.cache.get(config.canais.aprovacao);
        if (!aprovacaoChannel || aprovacaoChannel.type !== ChannelType.GuildText) {
          await interaction.reply({
            content: 'Canal de aprovação não encontrado.',
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const mensagemAprovacao = await aprovacaoChannel.send({
          embeds: [buildVendaPendenteEmbed(venda)],
          components: [createAprovacaoButtons(vendaId)],
        });

        await dbRun(
          'UPDATE vendas SET mensagem_origem_id = ?, mensagem_aprovacao_id = ? WHERE id = ?',
          [mensagemOrigem.id, mensagemAprovacao.id, vendaId]
        );

        await interaction.reply({
          content: `✅ Venda #${vendaId} registrada e enviada para aprovação. Cargo detectado: ${cashbackInfo.cargo} (${cashbackInfo.porcentagem}%).`,
          flags: MessageFlags.Ephemeral,
        });

        await atualizarPainelAutomatico(interaction.guild.id);
        await logAdmin(
          interaction.guild,
          '📝 Nova venda registrada',
          `${interaction.user.tag} registrou a venda #${vendaId} no valor de ${formatMoney(venda.valor)}.`
        );
        return;
      }

      if (interaction.customId.startsWith('modal_recusar_venda:')) {
        const vendaId = Number(interaction.customId.split(':')[1]);
        const motivo = cleanText(interaction.fields.getTextInputValue('motivo_recusa_input'));
        const venda = await dbGet(
          'SELECT * FROM vendas WHERE id = ? AND guild_id = ?',
          [vendaId, interaction.guild.id]
        );

        if (!venda) {
          await interaction.reply({ content: 'Venda não encontrada.', flags: MessageFlags.Ephemeral });
          return;
        }

        if (venda.status !== 'pendente') {
          await interaction.reply({
            content: `Essa venda já está como ${venda.status}.`,
            flags: MessageFlags.Ephemeral,
          });
          return;
        }

        const recusadoEm = agoraISO();
        await dbRun(
          `UPDATE vendas
           SET status = 'recusada', recusado_em = ?, recusado_por_id = ?, recusado_por_nome = ?, motivo_recusa = ?
           WHERE id = ?`,
          [recusadoEm, interaction.user.id, interaction.user.tag, motivo, vendaId]
        );

        const vendaAtualizada = await dbGet('SELECT * FROM vendas WHERE id = ?', [vendaId]);

        try {
          const aprovacaoChannel = interaction.guild.channels.cache.get(config.canais.aprovacao);
          if (
            aprovacaoChannel &&
            aprovacaoChannel.type === ChannelType.GuildText &&
            vendaAtualizada.mensagem_aprovacao_id
          ) {
            const msgAprov = await aprovacaoChannel.messages.fetch(vendaAtualizada.mensagem_aprovacao_id);
            await msgAprov.edit({
              embeds: [buildVendaRecusadaEmbed(vendaAtualizada)],
              components: [createDisabledAprovacaoButtons('recusada')],
            });
          }
        } catch (e) {
          console.error('Erro ao editar mensagem de aprovação na recusa:', e.message);
        }

        if (vendaAtualizada.mensagem_origem_id && vendaAtualizada.canal_origem_id) {
          try {
            const canalOrigem = interaction.guild.channels.cache.get(vendaAtualizada.canal_origem_id);
            if (canalOrigem && canalOrigem.type === ChannelType.GuildText) {
              const msgOrigem = await canalOrigem.messages.fetch(vendaAtualizada.mensagem_origem_id);
              await msgOrigem.edit({
                content: `❌ Venda #${vendaId} recusada por ${interaction.user.tag}.`,
                embeds: [buildVendaRecusadaEmbed(vendaAtualizada)],
              });
            }
          } catch (e) {
            console.error('Erro ao editar mensagem original da recusa:', e.message);
          }
        }

        await interaction.reply({
          content: `Venda #${vendaId} recusada com sucesso.`,
          flags: MessageFlags.Ephemeral,
        });

        await atualizarPainelAutomatico(interaction.guild.id);
        await logAdmin(
          interaction.guild,
          '❌ Venda recusada',
          `${interaction.user.tag} recusou a venda #${vendaId}. Motivo: ${motivo}`,
          0xe74c3c
        );
        return;
      }
    }
  } catch (err) {
    console.error('Erro na interaction:', err);
    if (interaction.deferred || interaction.replied) {
      await interaction.followUp({
        content: 'Ocorreu um erro ao processar a ação.',
        flags: MessageFlags.Ephemeral,
      }).catch(() => null);
    } else {
      await interaction.reply({
        content: 'Ocorreu um erro ao processar a ação.',
        flags: MessageFlags.Ephemeral,
      }).catch(() => null);
    }
  }
});

// =========================
// READY
// =========================
client.once('clientReady', async () => {
  console.log(`Bot conectado como ${client.user.tag}`);

  const meta = await dbGet('SELECT valor FROM configuracoes WHERE chave = ?', ['meta_diaria']);
  if (meta?.valor) config.fechamento.metaDiaria = Number(meta.valor);

  const painelCanalId = await getConfiguracao('painel_canal_id');
  const painelMensagemId = await getConfiguracao('painel_mensagem_id');

  if (painelCanalId) config.canais.painel = painelCanalId;
  if (painelMensagemId) config.painelMensagemId = painelMensagemId;

  cron.schedule(
    config.fechamento.cron,
    async () => {
      try {
        const guild = client.guilds.cache.get(config.guildId);
        if (!guild) return;
        await enviarFechamentoDiario(guild);
      } catch (e) {
        console.error('Erro no fechamento automático:', e);
      }
    },
    { timezone: config.fechamento.timezone }
  );

  setInterval(async () => {
    try {
      const guild = client.guilds.cache.get(config.guildId);
      if (!guild) return;
      await atualizarPainelAutomatico(guild.id);
    } catch (e) {
      console.error('Erro no auto update do painel:', e.message);
    }
  }, 5 * 60 * 1000);

  if (config.canais.painel && config.painelMensagemId) {
    await atualizarPainelAutomatico(config.guildId);
  }

  console.log('Agendamentos carregados.');
});

// =========================
// INICIALIZAÇÃO
// =========================
(async () => {
  try {
    validateEnv();
    await initDatabase();
    await registrarSlashCommands();
    await client.login(config.token);
  } catch (err) {
    console.error('Falha ao iniciar o bot:', err);
  }
})();