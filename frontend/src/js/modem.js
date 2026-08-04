export function getModuleLabel(model) {
    if (!model) return '—';
    const normalized = String(model).trim().toUpperCase();
    if (normalized === 'EC20F' || normalized === 'EC25' || normalized === 'A7630C-LANS' || normalized === 'A7670C-LANS') {
        return '4G';
    }
    return model;
}