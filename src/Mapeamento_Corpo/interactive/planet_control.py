import cv2
import mediapipe as mp
import numpy as np

PLANETA_PNG_PATH = "assets/images/planeta_terra.png"

def carregar_planeta_png(caminho_png, tamanho=(200, 200)):
    # tamanho base já maior
    img = cv2.imread(caminho_png, cv2.IMREAD_UNCHANGED)
    if img is None:
        return None
    img = cv2.resize(img, tamanho)
    return img

def gerar_frames_rotacionados(planeta_png, num_frames=36, max_angle=20):
    """
    Gera uma animação de 'balanço' lateral do planeta.
    Vai de -max_angle até +max_angle e volta, criando loop suave.
    """
    h, w = planeta_png.shape[:2]
    center = (w // 2, h // 2)
    frames = []

    for i in range(num_frames):
        t = i / (num_frames - 1)  # 0 -> 1
        angle = -max_angle + 2 * max_angle * t  # -max_angle -> +max_angle
        M = cv2.getRotationMatrix2D(center, angle, 1.0)
        rotated = cv2.warpAffine(
            planeta_png,
            M,
            (w, h),
            flags=cv2.INTER_LINEAR,
            borderMode=cv2.BORDER_CONSTANT,
            borderValue=(0, 0, 0, 0),  # fundo transparente
        )
        frames.append(rotated)

    frames += frames[::-1]
    return frames

def overlay_transparente(background, overlay_rgba, x, y):
    """
    Desenha overlay_rgba (BGRA) em background (BGR) na posição (x, y),
    respeitando o canal alpha do overlay.
    """
    h, w = overlay_rgba.shape[:2]

    if (
        y < 0
        or x < 0
        or y + h > background.shape[0]
        or x + w > background.shape[1]
    ):
        return background

    roi = background[y : y + h, x : x + w]

    overlay_bgr = overlay_rgba[:, :, :3]
    overlay_alpha = overlay_rgba[:, :, 3] / 255.0
    alpha = overlay_alpha[:, :, np.newaxis]

    background[y : y + h, x : x + w] = (
        roi * (1 - alpha) + overlay_bgr * alpha
    ).astype(np.uint8)

    return background

def contar_dedos_mao_esquerda(hand_landmarks, mp_hands):
    """
    Heurística simples: conta quantos dedos estão levantados.
    Se a maioria estiver levantada, mão 'aberta'; se poucos, 'fechada'.
    """
    if hand_landmarks is None:
        return 0

    tips_ids = [
        mp_hands.HandLandmark.THUMB_TIP,
        mp_hands.HandLandmark.INDEX_FINGER_TIP,
        mp_hands.HandLandmark.MIDDLE_FINGER_TIP,
        mp_hands.HandLandmark.RING_FINGER_TIP,
        mp_hands.HandLandmark.PINKY_TIP,
    ]

    pip_ids = [
        mp_hands.HandLandmark.THUMB_IP,
        mp_hands.HandLandmark.INDEX_FINGER_PIP,
        mp_hands.HandLandmark.MIDDLE_FINGER_PIP,
        mp_hands.HandLandmark.RING_FINGER_PIP,
        mp_hands.HandLandmark.PINKY_PIP,
    ]

    dedos_levantados = 0

    for tip_id, pip_id in zip(tips_ids, pip_ids):
        tip = hand_landmarks.landmark[tip_id]
        pip = hand_landmarks.landmark[pip_id]

        # se o tip está "acima" do pip (menor y), consideramos levantado
        if tip.y < pip.y:
            dedos_levantados += 1

    return dedos_levantados

def desenhar_teia_antigravidade(frame, left_hand, right_hand, mp_hands, w, h):
    """
    Desenha linhas com efeito 'neon' conectando os dedos das duas mãos.
    """
    if left_hand is None or right_hand is None:
        return

    tips = [
        mp_hands.HandLandmark.THUMB_TIP,
        mp_hands.HandLandmark.INDEX_FINGER_TIP,
        mp_hands.HandLandmark.MIDDLE_FINGER_TIP,
        mp_hands.HandLandmark.RING_FINGER_TIP,
        mp_hands.HandLandmark.PINKY_TIP,
    ]

    # Cores brilhantes BGR (Ciano, Magenta, Amarelo, Verde Neon, Laranja brilhante)
    cores_neon = [
        (255, 255, 0),   # Ciano (Polegar)
        (255, 0, 255),   # Magenta (Indicador)
        (0, 255, 255),   # Amarelo (Médio)
        (0, 255, 0),     # Verde (Anelar)
        (0, 165, 255)    # Laranja (Mindinho)
    ]

    for i, tip_id in enumerate(tips):
        pt1 = left_hand.landmark[tip_id]
        pt2 = right_hand.landmark[tip_id]

        x1, y1 = int(pt1.x * w), int(pt1.y * h)
        x2, y2 = int(pt2.x * w), int(pt2.y * h)

        # Linha Base grossa (Cor do Neon)
        cv2.line(frame, (x1, y1), (x2, y2), cores_neon[i], 4, cv2.LINE_AA)
        # Linha Fina Branca (Núcleo da energia)
        cv2.line(frame, (x1, y1), (x2, y2), (255, 255, 255), 1, cv2.LINE_AA)

        # Pontos brilhantes nas pontas dos dedos
        cv2.circle(frame, (x1, y1), 8, cores_neon[i], cv2.FILLED)
        cv2.circle(frame, (x2, y2), 8, cores_neon[i], cv2.FILLED)
        cv2.circle(frame, (x1, y1), 3, (255, 255, 255), cv2.FILLED)
        cv2.circle(frame, (x2, y2), 3, (255, 255, 255), cv2.FILLED)

def main():
    mp_hands = mp.solutions.hands
    mp_drawing = mp.solutions.drawing_utils

    # Ajuste de confiança para 0.7 e model_complexity=1 para rastreamento muito mais preciso
    hands = mp_hands.Hands(
        static_image_mode=False,
        max_num_hands=2,
        model_complexity=1,
        min_detection_confidence=0.7,
        min_tracking_confidence=0.7,
    )

    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Erro: não foi possível abrir a webcam.")
        return

    planeta_png = carregar_planeta_png(PLANETA_PNG_PATH, tamanho=(220, 220))
    if planeta_png is None:
        print("Erro: não foi possível carregar o PNG do planeta.")
        return

    planeta_frames = gerar_frames_rotacionados(
        planeta_png, num_frames=36, max_angle=20
    )
    frame_idx = 0
    frame_hold = 0
    HOLD_MAX = 3  # controla a velocidade da animação

    OFFSET_Y = 140  # pixels para cima em relação à palma

    # suavização da posição do planeta (segue a mão com fluidez)
    suavizado_cx = None
    suavizado_cy = None
    alpha = 0.25  # 0.1 = muito suave; 0.5 = rápido

    while True:
        ret, frame = cap.read()
        if not ret:
            print("Erro ao capturar frame.")
            break

        h_frame, w_frame, _ = frame.shape

        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        results = hands.process(frame_rgb)

        left_hand_landmarks = None
        right_hand_landmarks = None

        if results.multi_hand_landmarks and results.multi_handedness:
            for idx, hand_landmarks in enumerate(results.multi_hand_landmarks):
                hand_label = results.multi_handedness[idx].classification[0].label

                if hand_label == "Left" and left_hand_landmarks is None:
                    left_hand_landmarks = hand_landmarks
                elif hand_label == "Right" and right_hand_landmarks is None:
                    right_hand_landmarks = hand_landmarks

                # DESENHAR as mãos (rastreamento visível)
                mp_drawing.draw_landmarks(
                    frame,
                    hand_landmarks,
                    mp_hands.HAND_CONNECTIONS,
                )

        # Desenhar o Efeito de Antigravidade / Energia se as duas mãos estiverem na tela
        desenhar_teia_antigravidade(
            frame, left_hand_landmarks, right_hand_landmarks, mp_hands, w_frame, h_frame
        )

        # 1) pinça da mão DIREITA controla escala (sensível)
        scale_factor = 1.5  # planeta já começa maior
        if right_hand_landmarks is not None:
            thumb_tip = right_hand_landmarks.landmark[
                mp_hands.HandLandmark.THUMB_TIP
            ]
            index_tip = right_hand_landmarks.landmark[
                mp_hands.HandLandmark.INDEX_FINGER_TIP
            ]

            dx = (thumb_tip.x - index_tip.x) * w_frame
            dy = (thumb_tip.y - index_tip.y) * h_frame
            dist = (dx ** 2 + dy ** 2) ** 0.5

            # aumenta a sensibilidade (divisor menor)
            scale_factor = dist / 120.0
            scale_factor = max(0.7, min(scale_factor, 3.0))

        # 2) mão esquerda aberta/fechada controla aparição do planeta
        desenhar_planeta = False
        if left_hand_landmarks is not None:
            dedos_levantados = contar_dedos_mao_esquerda(
                left_hand_landmarks, mp_hands
            )

            # se 4 ou 5 dedos levantados -> mão aberta (planet ON)
            if dedos_levantados >= 4:
                desenhar_planeta = True

        if left_hand_landmarks is not None and desenhar_planeta:
            palm_idx = mp_hands.HandLandmark.MIDDLE_FINGER_MCP
            lx = left_hand_landmarks.landmark[palm_idx].x
            ly = left_hand_landmarks.landmark[palm_idx].y

            cx_raw = int(lx * w_frame)
            cy_raw = int(ly * h_frame)

            # suavização exponencial da posição
            if suavizado_cx is None:
                suavizado_cx = cx_raw
                suavizado_cy = cy_raw
            else:
                suavizado_cx = int(alpha * cx_raw + (1 - alpha) * suavizado_cx)
                suavizado_cy = int(alpha * cy_raw + (1 - alpha) * suavizado_cy)

            cx = suavizado_cx
            cy = suavizado_cy
            cy_offset = cy - OFFSET_Y

            base_planet = planeta_frames[frame_idx]
            h_p, w_p, _ = base_planet.shape

            new_w = int(w_p * scale_factor)
            new_h = int(h_p * scale_factor)

            if new_w > 0 and new_h > 0:
                planeta_scaled = cv2.resize(
                    base_planet,
                    (new_w, new_h),
                    interpolation=cv2.INTER_LINEAR,
                )

                x = cx - new_w // 2
                y = cy_offset - new_h // 2

                frame = overlay_transparente(frame, planeta_scaled, x, y)

            frame_hold += 1
            if frame_hold >= HOLD_MAX:
                frame_hold = 0
                frame_idx = (frame_idx + 1) % len(planeta_frames)
        else:
            # se a mão esquerda sumir ou fechar, reseta suavização
            suavizado_cx = None
            suavizado_cy = None

        cv2.imshow("Planeta e Efeito Antigravidade (Neon)", frame)

        if cv2.waitKey(1) & 0xFF == ord("q"):
            break

    cap.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()