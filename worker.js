// worker.js
importScripts('https://cdnjs.cloudflare.com/ajax/libs/PapaParse/5.4.1/papaparse.min.js');

function getWeekEndingSat(dateStr) {
    if (!dateStr) return 'Unknown';
    let d = new Date(dateStr);
    if (isNaN(d.getTime())) return 'Unknown';
    d.setDate(d.getDate() + (6 - d.getDay()));
    return d.toISOString().split('T')[0];
}

// 🛠️ REVERSED STATE MAPPING (Abbreviations to Full Title-Case Names)
const STATE_MAP = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", 
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia", 
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", 
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", 
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", 
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", 
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", 
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", 
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", 
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", 
    "DC": "District of Columbia", "PR": "Puerto Rico"
};

function cleanState(s) {
    if (!s) return 'Unknown';
    let clean = s.toString().trim().toUpperCase();
    if (clean.length === 0) return 'Unknown';
    if (STATE_MAP[clean]) return STATE_MAP[clean];
    let match = Object.values(STATE_MAP).find(name => name.toUpperCase() === clean);
    if (match) return match;
    return 'Unknown';
}

self.onmessage = function(e) {
    const payload = e.data;

    if (payload.action === 'PROCESS_CSV') {
        const file = payload.file;
        const mappingConfig = payload.mappingConfig || {};
        
        const safeMapping = {
            connects: Array.isArray(mappingConfig.connects) ? mappingConfig.connects : [],
            successes: Array.isArray(mappingConfig.successes) ? mappingConfig.successes : [],
            noAnswers: Array.isArray(mappingConfig.noAns) ? mappingConfig.noAns : [],
            ansMac: Array.isArray(mappingConfig.ansMac) ? mappingConfig.ansMac : [],
            badNum: Array.isArray(mappingConfig.badNum) ? mappingConfig.badNum : []
        };
        
        const domainAnis = new Set(payload.domainAnis || []);
        const rawFilters = payload.filters || {};
        const filters = {
            months: Array.isArray(rawFilters.months) ? rawFilters.months : [],
            weeks: Array.isArray(rawFilters.weeks) ? rawFilters.weeks : [],
            dates: Array.isArray(rawFilters.dates) ? rawFilters.dates : [],
            campaigns: Array.isArray(rawFilters.campaigns) ? rawFilters.campaigns : [],
            lists: Array.isArray(rawFilters.lists) ? rawFilters.lists : [],
            domainAnis: Array.isArray(rawFilters.domainAnis) ? rawFilters.domainAnis : [],
            areaCodes: Array.isArray(rawFilters.areaCodes) ? rawFilters.areaCodes : [],
            anis: Array.isArray(rawFilters.anis) ? rawFilters.anis : []
        };

        const startTime = performance.now();
        let rowCount = 0;
        let matchedCount = 0;
        
        let masterData = {
            global_kpis: {
                totalCalls: 0, outbound: 0, manual: 0, preview: 0,
                contacted: 0, liveConnects: 0, outboundLiveConnects: 0, abandons: 0,
                totalTalkTime: 0, totalHandleTime: 0, totalAcw: 0,
                sdCalls: 0, noAnswers: 0,
                userConnects: 0, userSuccesses: 0, userAnsMac: 0, userBadNum: 0
            },
            campaigns: {}, lists: {}, anis: {}, states: {},
            dailyMetrics: {}, weeklyMetrics: {}, globalDispos: {},
            globalHourly: {},
            workbenchAlerts: [],
            dnisMetrics: {} // 🛠️ NEW: Toxic Lead Tracking
        };

        let uniqueFilters = { months: new Set(), weeks: new Set(), dates: new Set(), campaigns: new Set(), lists: new Set(), domainAnis: new Set(), areaCodes: new Set(), anis: new Set() };
        const isInitialRun = (filters.months.length === 0 && filters.weeks.length === 0 && filters.dates.length === 0 && filters.campaigns.length === 0 && filters.lists.length === 0 && filters.domainAnis.length === 0 && filters.areaCodes.length === 0 && filters.anis.length === 0);

        Papa.parse(file, {
            header: true,
            skipEmptyLines: true,
            worker: false,
            step: function(results) {
                const row = results.data;
                rowCount++;

                const disp = row['DISPOSITION'] || row['Call Result'] || 'Unknown';
                const callType = (row['CALL TYPE'] || '').toLowerCase();
                const campaign = row['CAMPAIGN'] || 'Unknown Campaign';
                const listName = row['LIST NAME'] || 'Unknown List';
                const ani = row['ANI'] || 'Unknown ANI';
                const state = cleanState(row['ANI STATE']);
                const dnis = row['DNIS'] || '';
                const dateRaw = row['DATE'] || '';
                const hourStr = row['HOUR'] || '';
                
                let cleanDate = dateRaw.split(' ')[0] || 'Unknown';
                let monthStr = cleanDate !== 'Unknown' ? cleanDate.substring(0,7) : 'Unknown';
                let weekSat = getWeekEndingSat(cleanDate);
                
                let isDomainStr = domainAnis.has(ani) ? 'Yes' : 'No';
                let areaCode = ani.length >= 3 ? ani.substring(0,3) : 'Unknown';

                if (isInitialRun) {
                    if (monthStr !== 'Unknown') uniqueFilters.months.add(monthStr);
                    if (weekSat !== 'Unknown') uniqueFilters.weeks.add(weekSat);
                    if (cleanDate !== 'Unknown') uniqueFilters.dates.add(cleanDate);
                    uniqueFilters.campaigns.add(campaign);
                    uniqueFilters.lists.add(listName);
                    uniqueFilters.domainAnis.add(isDomainStr);
                    uniqueFilters.areaCodes.add(areaCode);
                    uniqueFilters.anis.add(ani);
                }

                if (!isInitialRun) {
                    if (filters.months.length > 0 && !filters.months.includes(monthStr)) return;
                    if (filters.weeks.length > 0 && !filters.weeks.includes(weekSat)) return;
                    if (filters.dates.length > 0 && !filters.dates.includes(cleanDate)) return;
                    if (filters.campaigns.length > 0 && !filters.campaigns.includes(campaign)) return;
                    if (filters.lists.length > 0 && !filters.lists.includes(listName)) return;
                    if (filters.domainAnis.length > 0 && !filters.domainAnis.includes(isDomainStr)) return;
                    if (filters.areaCodes.length > 0 && !filters.areaCodes.includes(areaCode)) return;
                    if (filters.anis.length > 0 && !filters.anis.includes(ani)) return;
                }
                
                matchedCount++;

                const parseTime = (timeStr) => {
                    if (!timeStr) return 0;
                    const p = timeStr.split(':');
                    return p.length === 3 ? (+p[0])*3600 + (+p[1])*60 + (+p[2]) : 0;
                };
                
                const talkTimeSec = parseTime(row['TALK TIME']);
                const handleTimeSec = parseTime(row['HANDLE TIME']);
                const acwSec = parseTime(row['AFTER CALL WORK TIME']);

                const isContacted = parseInt(row['CONTACTED']) === 1;
                const isLiveConnect = parseInt(row['LIVE CONNECT']) === 1;
                const isAbandoned = parseInt(row['ABANDONED']) === 1;

                const isUserConnect = safeMapping.connects.includes(disp);
                const isUserSuccess = safeMapping.successes.includes(disp);
                const isUserAnsMac = safeMapping.ansMac.includes(disp);
                const isUserBadNum = safeMapping.badNum.includes(disp);
                const isNoAnswer = safeMapping.noAnswers.includes(disp) || disp === 'No Answer';
                const isSdCall = talkTimeSec < 6 && talkTimeSec > 0;

                let k = masterData.global_kpis;
                k.totalCalls++;
                if (callType === 'outbound') k.outbound++;
                if (callType === 'manual') k.manual++;
                if (callType === 'preview') k.preview++;
                if (isContacted) k.contacted++;
                if (isLiveConnect) {
                    k.liveConnects++;
                    if (callType === 'outbound') k.outboundLiveConnects++;
                }
                if (isAbandoned) k.abandons++;
                
                k.totalTalkTime += talkTimeSec; k.totalHandleTime += handleTimeSec; k.totalAcw += acwSec;
                if (isSdCall) k.sdCalls++; if (isNoAnswer) k.noAnswers++;
                if (isUserConnect) k.userConnects++; if (isUserSuccess) k.userSuccesses++;
                if (isUserAnsMac) k.userAnsMac++; if (isUserBadNum) k.userBadNum++;

                masterData.globalDispos[disp] = (masterData.globalDispos[disp] || 0) + 1;

                // 🛠️ NEW: Record Toxic Lead data directly per DNIS
                if (dnis) {
                    if(!masterData.dnisMetrics[dnis]) masterData.dnisMetrics[dnis] = { calls: 0, badNum: 0 };
                    masterData.dnisMetrics[dnis].calls++;
                    if(isUserBadNum) masterData.dnisMetrics[dnis].badNum++;
                }

                const initBucket = (obj, key) => {
                    if (!obj[key]) obj[key] = {
                        calls: 0, outbound: 0, connects: 0, successes: 0, contacted: 0, abandons: 0, outboundLiveConnects: 0,
                        ansMac: 0, badNum: 0, sdCalls: 0, noAnswers: 0,
                        dnisCounts: {}, uniqueDates: new Set(), dispos: {}, daily: {}, hourly: {}
                    };
                };

                initBucket(masterData.campaigns, campaign);
                masterData.campaigns[campaign].calls++;
                if (callType === 'outbound') masterData.campaigns[campaign].outbound++;
                if (isContacted) masterData.campaigns[campaign].contacted++;
                if (isUserConnect) masterData.campaigns[campaign].connects++;
                if (isUserSuccess) masterData.campaigns[campaign].successes++;
                if (isAbandoned) masterData.campaigns[campaign].abandons++;
                if (isLiveConnect && callType === 'outbound') masterData.campaigns[campaign].outboundLiveConnects++;
                masterData.campaigns[campaign].dispos[disp] = (masterData.campaigns[campaign].dispos[disp] || 0) + 1;
                
                if (hourStr) {
                    let hr = parseInt(hourStr);
                    if (!masterData.campaigns[campaign].hourly[hr]) masterData.campaigns[campaign].hourly[hr] = { calls: 0, abandons: 0, outboundLiveConnects: 0 };
                    masterData.campaigns[campaign].hourly[hr].calls++;
                    if (isAbandoned) masterData.campaigns[campaign].hourly[hr].abandons++;
                    if (isLiveConnect && callType === 'outbound') masterData.campaigns[campaign].hourly[hr].outboundLiveConnects++;
                }

                if (dnis) {
                    if(!masterData.campaigns[campaign].uniqueLeadsSet) masterData.campaigns[campaign].uniqueLeadsSet = new Set();
                    masterData.campaigns[campaign].uniqueLeadsSet.add(dnis);
                }

                if (state && state !== 'Unknown') {
                    initBucket(masterData.states, state);
                    masterData.states[state].calls++;
                    if (isContacted) masterData.states[state].contacted++;
                    if (isUserConnect) masterData.states[state].connects++;
                    if (isUserSuccess) masterData.states[state].successes++;
                    if (isAbandoned) masterData.states[state].abandons++;
                    if (isLiveConnect && callType === 'outbound') masterData.states[state].outboundLiveConnects++;
                    
                    if (hourStr) {
                        let hr = parseInt(hourStr);
                        if (!masterData.states[state].hourly[hr]) masterData.states[state].hourly[hr] = { calls: 0, contacted: 0, connects: 0, successes: 0 };
                        masterData.states[state].hourly[hr].calls++;
                        if (isContacted) masterData.states[state].hourly[hr].contacted++;
                        if (isUserConnect) masterData.states[state].hourly[hr].connects++;
                        if (isUserSuccess) masterData.states[state].hourly[hr].successes++;
                    }
                }

                initBucket(masterData.lists, listName);
                masterData.lists[listName].calls++;
                if (isContacted) masterData.lists[listName].contacted++;
                if (isUserConnect) masterData.lists[listName].connects++;
                if (isUserSuccess) masterData.lists[listName].successes++;
                if (isUserBadNum) masterData.lists[listName].badNum++;
                if (dnis) masterData.lists[listName].dnisCounts[dnis] = (masterData.lists[listName].dnisCounts[dnis] || 0) + 1;

                initBucket(masterData.anis, ani);
                masterData.anis[ani].calls++;
                if (isContacted) masterData.anis[ani].contacted++;
                if (isUserConnect) masterData.anis[ani].connects++;
                if (isUserAnsMac) masterData.anis[ani].ansMac++;
                if (isUserBadNum) masterData.anis[ani].badNum++;
                if (isSdCall) masterData.anis[ani].sdCalls++;
                if (isNoAnswer) masterData.anis[ani].noAnswers++;
                if (dateRaw) masterData.anis[ani].uniqueDates.add(dateRaw);
                masterData.anis[ani].isDomain = isDomainStr;
                masterData.anis[ani].areaCode = areaCode;

                if (cleanDate !== 'Unknown') {
                    if (!masterData.anis[ani].daily[cleanDate]) {
                        masterData.anis[ani].daily[cleanDate] = { calls: 0, contacted: 0, connects: 0 };
                    }
                    let dailyAniBucket = masterData.anis[ani].daily[cleanDate];

                    dailyAniBucket.calls++;
                    if (isContacted) dailyAniBucket.contacted++;
                    if (isUserConnect) dailyAniBucket.connects++;
                }

                if (cleanDate !== 'Unknown') {
                    if (!masterData.dailyMetrics[cleanDate]) masterData.dailyMetrics[cleanDate] = { calls: 0, contacted: 0, connects: 0, successes: 0, abandons: 0, outboundLiveConnects: 0 };
                    masterData.dailyMetrics[cleanDate].calls++;
                    if (isContacted) masterData.dailyMetrics[cleanDate].contacted++;
                    if (isUserConnect) masterData.dailyMetrics[cleanDate].connects++;
                    if (isUserSuccess) masterData.dailyMetrics[cleanDate].successes++;
                    if (isAbandoned) masterData.dailyMetrics[cleanDate].abandons++;
                    if (isLiveConnect && callType === 'outbound') masterData.dailyMetrics[cleanDate].outboundLiveConnects++;
                    
                    if (weekSat !== 'Unknown') {
                        if (!masterData.weeklyMetrics[weekSat]) {
                            masterData.weeklyMetrics[weekSat] = { calls: 0, contacted: 0, connects: 0, successes: 0, abandons: 0, hourly: {} };
                        }
                        let w = masterData.weeklyMetrics[weekSat];
                        w.calls++;
                        if (isContacted) w.contacted++;
                        if (isUserConnect) w.connects++;
                        if (isUserSuccess) w.successes++;
                        if (isAbandoned) w.abandons++;

                        if (hourStr) {
                            let hr = parseInt(hourStr);
                            if (!w.hourly[hr]) w.hourly[hr] = { calls: 0, contacted: 0, connects: 0, successes: 0 };
                            w.hourly[hr].calls++;
                            if (isContacted) w.hourly[hr].contacted++;
                            if (isUserConnect) w.hourly[hr].connects++;
                            if (isUserSuccess) w.hourly[hr].successes++;
                        }
                    }
                }
                
                if (hourStr) {
                    let hr = parseInt(hourStr);
                    if (!masterData.globalHourly[hr]) masterData.globalHourly[hr] = { calls: 0, abandons: 0, outboundLiveConnects: 0, contacted: 0, connects: 0, successes: 0 };
                    masterData.globalHourly[hr].calls++;
                    if (isAbandoned) masterData.globalHourly[hr].abandons++;
                    if (isLiveConnect && callType === 'outbound') masterData.globalHourly[hr].outboundLiveConnects++;
                    if (isContacted) masterData.globalHourly[hr].contacted++;
                    if (isUserConnect) masterData.globalHourly[hr].connects++;
                    if (isUserSuccess) masterData.globalHourly[hr].successes++;
                }

            },
            
            complete: function() {
                let finalData = masterData;
                let k = finalData.global_kpis;

                k.avgTalkTime = k.totalCalls > 0 ? (k.totalTalkTime / k.totalCalls) : 0;
                k.avgHandleTime = k.totalCalls > 0 ? (k.totalHandleTime / k.totalCalls) : 0;
                k.avgAcw = k.totalCalls > 0 ? (k.totalAcw / k.totalCalls) : 0;
                k.connectRate = k.totalCalls > 0 ? (k.userConnects / k.totalCalls) * 100 : 0;
                k.successRate = k.userConnects > 0 ? (k.userSuccesses / k.userConnects) * 100 : 0;
                k.abandonRate = k.outboundLiveConnects > 0 ? (k.abandons / k.outboundLiveConnects) * 100 : 0;

                // Stat collectors for Outlier Engine
                let listConnRates = [], campConnRates = [], campAbnRates = [], aniDpd = [];
                let totalListAttempts = 0, totalUniqueDnisGlobal = 0;

                // --- 1. LISTS PROCESSING ---
                for (let list in finalData.lists) {
                    let l = finalData.lists[list];
                    let dCounts = Object.values(l.dnisCounts);
                    if (dCounts.length > 0) {
                        let sum = dCounts.reduce((a, b) => a + b, 0);
                        l.uniqueLeads = dCounts.length;
                        l.avgAttempts = sum / dCounts.length;
                        l.maxAttempts = Math.max(...dCounts);
                        l.minAttempts = Math.min(...dCounts);
                        totalListAttempts += sum; totalUniqueDnisGlobal += dCounts.length;
                    } else {
                        l.uniqueLeads = 0; l.avgAttempts = 0; l.maxAttempts = 0; l.minAttempts = 0;
                    }
                    delete l.dnisCounts;

                    if (l.calls > 10) listConnRates.push(l.connects / l.calls);
                }
                k.globalAvgAttempts = totalUniqueDnisGlobal > 0 ? (totalListAttempts / totalUniqueDnisGlobal) : 0;

                // --- 2. CAMPAIGNS PROCESSING ---
                for (let camp in finalData.campaigns) {
                    let c = finalData.campaigns[camp];
                    c.uniqueLeads = c.uniqueLeadsSet ? c.uniqueLeadsSet.size : 0;
                    c.avgAttempts = c.uniqueLeads > 0 ? (c.calls / c.uniqueLeads) : 0;
                    delete c.uniqueLeadsSet;

                    let maxAbnCount = -1; let peakHrStr = '--:--'; let peakAbnRate = 0;
                    for (let hr = 0; hr < 24; hr++) {
                        if (c.hourly && c.hourly[hr]) {
                            let hData = c.hourly[hr];
                            if (hData.abandons > maxAbnCount && hData.abandons > 0) {
                                maxAbnCount = hData.abandons; peakHrStr = `${hr.toString().padStart(2, '0')}:00`;
                                peakAbnRate = hData.outboundLiveConnects > 0 ? (hData.abandons / hData.outboundLiveConnects) * 100 : 0;
                            }
                        }
                    }
                    c.peakAbandonHour = peakHrStr; c.peakAbandonRate = peakAbnRate;

                    if (c.calls > 10) {
                        campConnRates.push(c.connects / c.calls);
                        campAbnRates.push(c.outboundLiveConnects > 0 ? (c.abandons / c.outboundLiveConnects) : 0);
                    }
                }

                // --- 3. ANI RISK SCORING & 🛠️ NEW AREA CODE STATS ---
                let areaCodeStats = {}; // 🛠️ NEW: Aggregate Area Code usage vs inventory
                
                for (let ani in finalData.anis) {
                    let a = finalData.anis[ani];
                    let calls = a.calls;
                    a.dialsPerDay = calls / (a.uniqueDates.size > 0 ? a.uniqueDates.size : 1);
                    delete a.uniqueDates;
                    
                    let ctcRate = calls > 0 ? (a.contacted / calls) : 0;
                    let badNumPct = calls > 0 ? (a.badNum / calls) : 0;
                    let ansMacPct = calls > 0 ? (a.ansMac / calls) : 0;
                    let sdPct = calls > 0 ? (a.sdCalls / calls) : 0;

                    let score = 0;
                    if (calls > 1000) score += 30; else if (calls > 500) score += 15;
                    if (a.dialsPerDay > 100) score += 20; else if (a.dialsPerDay > 50) score += 10;
                    if (ansMacPct > 0.60) score += 25; else if (ansMacPct > 0.40) score += 15;
                    if (ctcRate < 0.15) score += 20; else if (ctcRate < 0.25) score += 10;
                    if (badNumPct > 0.10) score += 25;
                    a.riskScore = Math.min(Math.round(score), 100);

                    if (calls < 50) { a.status = "Monitor"; a.action = "Monitor"; }
                    else if (a.riskScore >= 75) { a.status = "Burned"; a.action = "Quarantine"; }
                    else if (a.riskScore >= 45) { a.status = "At Risk"; a.action = "Rotate"; }
                    else { a.status = "Clean"; a.action = "Keep Active"; }

                    if (calls > 5) aniDpd.push(a.dialsPerDay); // For outlier math
                    
                    // 🛠️ NEW: Tally area code usage
                    let ac = a.areaCode;
                    if (ac && ac !== 'Unknown') {
                        if (!areaCodeStats[ac]) areaCodeStats[ac] = { dpd: 0, anis: 0 };
                        areaCodeStats[ac].dpd += a.dialsPerDay;
                        areaCodeStats[ac].anis += 1;
                    }
                }

                // --- 4. THE OUTLIER ENGINE (Workbench Alert Gen) ---
                const getStats = (arr) => {
                    if(arr.length === 0) return { mean: 0, stdDev: 0 };
                    let mean = arr.reduce((acc, val) => acc + val, 0) / arr.length;
                    let variance = arr.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / arr.length;
                    return { mean: mean, stdDev: Math.sqrt(variance) };
                };

                let listStats = getStats(listConnRates);
                let campStats = getStats(campConnRates);
                let campAbnStats = getStats(campAbnRates);
                let aniStats = getStats(aniDpd);

                let critAniVelocity = Math.max(30, aniStats.mean + (2.5 * aniStats.stdDev)); 
                let highAniVelocity = Math.max(20, aniStats.mean + (1.5 * aniStats.stdDev));

                for (let camp in finalData.campaigns) {
                    let c = finalData.campaigns[camp];
                    if (c.calls < 20) continue; 
                    let cRate = c.connects / c.calls;
                    let aRate = c.outboundLiveConnects > 0 ? (c.abandons / c.outboundLiveConnects) : 0;
                    
                    if (cRate < (campStats.mean - campStats.stdDev) && cRate < 0.05) { 
                        finalData.workbenchAlerts.push({ type: 'Campaign', id: camp, metric: 'Connect Rate', value: `${(cRate*100).toFixed(1)}%`, severity: 'Critical', desc: `Performing significantly below the network average of ${(campStats.mean*100).toFixed(1)}%.` });
                    }
                    if (aRate > 0.05 && aRate > (campAbnStats.mean + (1.5 * campAbnStats.stdDev))) {
                        finalData.workbenchAlerts.push({ type: 'Campaign', id: camp, metric: 'Abandon Rate', value: `${(aRate*100).toFixed(1)}%`, severity: 'Critical', desc: `Critical TCPA Risk. Spiking abnormally high compared to network.` });
                    }
                }

                for (let list in finalData.lists) {
                    let l = finalData.lists[list];
                    if (l.calls < 20) continue;
                    let cRate = l.connects / l.calls;
                    if (cRate < (listStats.mean - listStats.stdDev)) {
                        finalData.workbenchAlerts.push({ type: 'List', id: list, metric: 'Quality', value: `${(cRate*100).toFixed(1)}%`, severity: 'High', desc: `List quality is highly degraded vs network average.` });
                    }
                    if (l.avgAttempts > (k.globalAvgAttempts * 2) && l.avgAttempts > 3) {
                        finalData.workbenchAlerts.push({ type: 'List', id: list, metric: 'Fatigue', value: `${l.avgAttempts.toFixed(1)} Att`, severity: 'High', desc: `List is heavily dialed.` });
                    }
                }

                for (let ani in finalData.anis) {
                    let a = finalData.anis[ani];
                    let calls = a.calls;
                    let ansMacPct = calls > 0 ? (a.ansMac / calls) : 0;
                    let badNumPct = calls > 0 ? (a.badNum / calls) : 0;
                    
                    if (a.dialsPerDay > critAniVelocity) {
                        finalData.workbenchAlerts.push({ type: 'ANI', id: ani, metric: 'Velocity', value: `${a.dialsPerDay.toFixed(0)}/Day`, severity: 'Critical', desc: `Extreme Outlier. Dialing significantly faster than network avg.` });
                    } else if (a.dialsPerDay > highAniVelocity && ansMacPct > 0.50) {
                        finalData.workbenchAlerts.push({ type: 'ANI', id: ani, metric: 'Spam Risk', value: `${(ansMacPct*100).toFixed(0)}% VM`, severity: 'High', desc: `High volume outlier combined with >50% Voicemail rate.` });
                    }
                }

                // 🛠️ NEW: Evaluate Area Code Saturation
                for (let ac in areaCodeStats) {
                    let stats = areaCodeStats[ac];
                    if (stats.dpd > 50) { // Only check active ACs
                        let saturation = stats.dpd / stats.anis;
                        if (saturation > 75) { 
                            finalData.workbenchAlerts.push({
                                type: 'Area Code',
                                id: `AC ${ac}`,
                                metric: 'Saturation',
                                value: `${saturation.toFixed(0)}/ANI`,
                                severity: saturation > 120 ? 'Critical' : 'High',
                                desc: `Inventory depletion. Pushing ${stats.dpd.toFixed(0)} dials/day via only ${stats.anis} local number(s).`
                            });
                        }
                    }
                }

                // 🛠️ NEW: Build Toxic Leads Payload
                finalData.toxicLeads = [];
                let toxicCount = 0;
                for (let dnis in finalData.dnisMetrics) {
                    let dm = finalData.dnisMetrics[dnis];
                    // 3 or more attempts, ALL resulting in Bad Numbers
                    if (dm.calls >= 3 && dm.badNum === dm.calls) {
                        toxicCount++;
                        if (finalData.toxicLeads.length < 500) { // Safety limit payload
                            finalData.toxicLeads.push({
                                dnis: dnis,
                                attempts: dm.calls
                            });
                        }
                    }
                }
                finalData.toxicLeadsCount = toxicCount;
                delete finalData.dnisMetrics; // Clean up massive dictionary from memory

                if (isInitialRun) {
                    finalData.filterOptions = {
                        months: Array.from(uniqueFilters.months).sort(),
                        weeks: Array.from(uniqueFilters.weeks).sort(),
                        dates: Array.from(uniqueFilters.dates).sort(),
                        campaigns: Array.from(uniqueFilters.campaigns).sort(),
                        lists: Array.from(uniqueFilters.lists).sort(),
                        domainAnis: Array.from(uniqueFilters.domainAnis).sort(),
                        areaCodes: Array.from(uniqueFilters.areaCodes).sort(),
                        anis: Array.from(uniqueFilters.anis).sort()
                    };
                }

                self.postMessage({
                    status: 'complete',
                    total: matchedCount,
                    time: performance.now() - startTime,
                    results: finalData
                });
            }
        });
    }
};