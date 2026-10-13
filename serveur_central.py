"""
serveur_central.py — LE SERVEUR CENTRAL (à mettre sur le serveur Oracle).

Rôle : c'est le "point de rendez-vous", comme les serveurs d'AnyDesk.
  - Chaque application qui se lance s'y connecte et reçoit un IDENTIFIANT à 9
    chiffres (ex. 482 193 077).
  - Pour contrôler quelqu'un, on envoie son ID : le serveur met les deux
    applications en relation et transmet l'écran et les commandes entre elles.

Le serveur ne regarde pas le contenu : il ne fait que transmettre.

Lancer sur le serveur Oracle :
    python3 serveur_central.py
Écoute par défaut sur le port 8765 (modifiable via la variable PORT).
"""

import asyncio
import json
import os
import random
import sys

try:
    from aiohttp import web, WSMsgType
except ImportError:
    print("Le module 'aiohttp' manque. Installe-le avec :")
    print("    pip3 install aiohttp")
    sys.exit(1)

PORT = int(os.environ.get("PORT", "8765"))

# id (str 9 chiffres) -> websocket de l'application
clients = {}
# websocket -> websocket du partenaire (session en cours)
peer = {}
# websocket -> son id
ids = {}


def new_id():
    while True:
        i = "".join(random.choice("0123456789") for _ in range(9))
        if i not in clients:
            return i


async def send_json(ws, obj):
    try:
        await ws.send_json(obj)
    except Exception:
        pass


async def ws_handler(request):
    ws = web.WebSocketResponse(max_msg_size=32 * 1024 * 1024, heartbeat=25)
    await ws.prepare(request)

    my_id = new_id()
    clients[my_id] = ws
    ids[ws] = my_id
    await send_json(ws, {"type": "your_id", "id": my_id})
    print(f"[+] client connecté, id={my_id} (total {len(clients)})")

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Essayer de lire un message de contrôle (register/connect).
                handled = False
                try:
                    m = json.loads(msg.data)
                    t = m.get("type")
                except Exception:
                    m, t = None, None

                if t == "register":
                    # déjà enregistré à la connexion ; on renvoie l'id au cas où
                    await send_json(ws, {"type": "your_id", "id": my_id})
                    handled = True
                elif t == "connect":
                    target = str(m.get("target", "")).replace(" ", "")
                    host = clients.get(target)
                    if host is None:
                        await send_json(ws, {"type": "error",
                                             "message": "Identifiant introuvable ou hors ligne."})
                    elif host is ws:
                        await send_json(ws, {"type": "error",
                                             "message": "Vous ne pouvez pas vous contrôler vous-même."})
                    elif ws in peer or host in peer:
                        await send_json(ws, {"type": "error",
                                             "message": "Une session est déjà en cours."})
                    else:
                        peer[ws] = host
                        peer[host] = ws
                        await send_json(host, {"type": "controller_joined"})
                    handled = True

                if not handled:
                    # Tout le reste (accepted, refused, ended, souris, clavier…)
                    # est transmis au partenaire tel quel.
                    p = peer.get(ws)
                    if p is not None and not p.closed:
                        try:
                            await p.send_str(msg.data)
                        except Exception:
                            pass

            elif msg.type == WSMsgType.BINARY:
                # Image (tuiles) -> transmise au partenaire.
                p = peer.get(ws)
                if p is not None and not p.closed:
                    try:
                        await p.send_bytes(msg.data)
                    except Exception:
                        pass
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        # Nettoyage à la déconnexion.
        p = peer.pop(ws, None)
        if p is not None:
            peer.pop(p, None)
            if not p.closed:
                await send_json(p, {"type": "peer_left"})
        clients.pop(my_id, None)
        ids.pop(ws, None)
        print(f"[-] client déconnecté, id={my_id} (total {len(clients)})")

    return ws


async def health(request):
    return web.Response(text="Serveur central OK")


def main():
    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/", health)
    print("=" * 50)
    print(" Serveur central démarré")
    print(f" Port : {PORT}")
    print(" Les applications s'y connectent pour obtenir un ID.")
    print(" Arrêter : Ctrl + C")
    print("=" * 50)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)


if __name__ == "__main__":
    main()
