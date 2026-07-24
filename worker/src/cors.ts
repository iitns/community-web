import type { Env } from './types';

function allowedOrigins(env: Env): string[] {
  return env.CORS_ALLOWED_ORIGINS.split(',').map((origin) => origin.trim()).filter(Boolean);
}

export function corsHeaders(request: Request, env: Env): HeadersInit {
  const origin = request.headers.get('Origin') ?? '';
  const headers: Record<string, string> = {
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
    Vary: 'Origin',
  };

  if (allowedOrigins(env).includes(origin)) {
    headers['Access-Control-Allow-Origin'] = origin;
  }

  return headers;
}

export function preflightResponse(request: Request, env: Env): Response {
  return new Response(null, { status: 204, headers: corsHeaders(request, env) });
}

export function jsonResponse(request: Request, env: Env, data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      ...corsHeaders(request, env),
    },
  });
}
