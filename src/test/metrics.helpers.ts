// Valor da serie com exatamente esses labels (ordem de labels irrelevante).
export function sampleValue(text: string, name: string, labels: Record<string, string>): number | undefined {
  const wanted = Object.entries(labels).map(([k, v]) => `${k}="${v}"`);
  for (const line of text.split("\n")) {
    const match = /^([a-zA-Z_:][\w:]*)\{(.*)\} (\S+)$/.exec(line);
    if (match?.[1] !== name) continue;
    const present = (match[2] ?? "").split(",");
    if (wanted.every((pair) => present.includes(pair))) return Number(match[3]);
  }
  return undefined;
}
