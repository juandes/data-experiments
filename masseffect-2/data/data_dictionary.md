# Mass Effect 2 data

Mass Effect 2 Legendary Edition data. All percentages use a 0–100 scale: `75.9` means 75.9%.

## Files

| File | Contents |
|---|---|
| [optional_recruitment.csv](optional_recruitment.csv) | Chart input: adjusted recruitment for five optional squadmates. |
| [adjusted_loyalty_missions.csv](adjusted_loyalty_missions.csv) | Chart input: adjusted completion for 12 loyalty missions. |
| [squad_survival_2021.csv](squad_survival_2021.csv) | Chart input: EA's survival rates for 12 squadmates. |
| [steam_early_progress_achievements.csv](steam_early_progress_achievements.csv) | Original opening-milestone percentages. |
| [steam_suicide_mission_achievements.csv](steam_suicide_mission_achievements.csv) | Original final-mission and completion percentages. |
| [steam_recruitment_achievements.csv](steam_recruitment_achievements.csv) | Original and adjusted recruitment percentages, with optional status. |
| [steam_loyalty_mission_achievements.csv](steam_loyalty_mission_achievements.csv) | Original loyalty-mission percentages. |

The adjusted files are derived from the original achievement files.

## Columns

| Column | Meaning |
|---|---|
| `character` | Squadmate name; `Samara / Morinth` is a shared mission achievement. |
| `achievement_name` | Steam achievement name. |
| `achievement_unlock_pct` | Original lifetime unlock percentage across Legendary Edition's Steam achievement population. |
| `survival_pct` | Individual survival percentage published by EA. |
| `estimated_recruitment_among_starters_pct` | Adjusted recruitment percentage using the formula below. |
| `estimated_loyalty_mission_among_starters_pct` | Adjusted loyalty-mission percentage using the same formula. |
| `optional_recruitment` | `yes` if this squadmate can be skipped while completing the unmodded story; otherwise `no`. |

## Adjustment

Steam percentages were collected **October 21, 2026** and rounded to one decimal. Adjusted values use the opening achievement, **Missing in Action (40.2%)**, as a proxy for starting ME2:

`adjusted percentage = ROUND(original achievement percentage / 40.2 * 100, 1)`

Example: Tali's recruitment estimate is `30.5 / 40.2 * 100 = 75.9%`.

## Limits

- Adjusted values assume consistently recorded prerequisite achievements. They estimate lifetime account progress, not single-playthrough outcomes. Missing unlocks include people who stopped playing before reaching a mission; they do not establish deliberate skipping or loyalty retained at the finale.
- Recruitment percentages are not rates among finishers. Accounts may play multiple runs, and equal rounded achievement rates do not prove identical outcomes.
- Grunt and Legion have required story missions but optional activation. The five optional recruits can each be skipped, although story progression still requires eight squadmates.
- Doppelganger can unlock with either Samara or Morinth. No One Left Behind covers the recruited combat squad, not every Normandy crew member. Against All Odds tracks Shepard's survival; Mission Accomplished tracks completion.
- EA survival figures come from a separate 2021 population. Sample size, exact denominator and repeat-run handling are undisclosed. The graphic does not explain Morinth's treatment. Do not combine these rates with Steam percentages or multiply them to estimate full-squad survival.

## Sources

- [Steam global achievements](https://steamcommunity.com/achievements/1328670?l=english): achievement names and percentages.
- [EA's 2021 Year in Gaming infographic](https://media.contentapi.ea.com/content/dam/eacom/common/ea-2021-year-in-gaming-final-wcredits.jpg): survival rates, published December 2021; stated collection window January 1–October 31, 2021 for non-Apex games.
- [Legendary Edition achievement guide](https://www.gamespot.com/articles/mass-effect-trophy-achievements/1100-6491526/): achievement meanings.
- Prima's [Warlord](https://primagames.com/eguides/mass-effect-2-eguide/walkthrough/walkthrough/dossier-the-warlord) and [Reaper IFF](https://primagames.com/eguides/mass-effect-2-eguide/walkthrough/walkthrough/derelict-reaper-acquire-reaper-iff) guides, [recruitment discussion](https://www.reddit.com/r/masseffect/comments/16sxwtz/) and [Collector Ship requirements](https://masseffect.fandom.com/wiki/Collector_Ship_%28mission%29): recruitment classifications, checked October 22, 2026.
