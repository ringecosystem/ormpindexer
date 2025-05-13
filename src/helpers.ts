export function compareHashString(hash1: string, hash2: string): boolean {
  return (
    hash1.toLowerCase().replace("0x", "") ===
    hash2.toLowerCase().replace("0x", "")
  );
}

export function stdHashString(hash: string): string {
  const lowerCaseHash = hash.toLowerCase();
  if (lowerCaseHash.startsWith("0x")) {
    return lowerCaseHash;
  }
  return "0x" + lowerCaseHash;
}
