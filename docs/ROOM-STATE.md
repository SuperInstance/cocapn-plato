# PLATO Room-State Protocol (WIP — lane-z3 checkpoint)

Recon-first checkpoint. Real citations to be locked at `ded68d0`.
Router: FastAPI `create_app` — `src/cocapn_plato/server/routes.py:75`.
Room model: `src/cocapn_plato/engine/rooms/room.py` (id, name, capacity, description, participants, created_at, state).
Participant: `src/cocapn_plato/engine/rooms/participant.py` (name, role, joined_at, last_active, message_count, history).
