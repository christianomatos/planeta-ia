import cv2
import mediapipe as mp
import numpy as np

mp_drawing = mp.solutions.drawing_utils
mp_holistic = mp.solutions.holistic
mp_face_mesh = mp.solutions.face_mesh


def calc_dist_xy(p1, p2, w, h):
    x1, y1 = int(p1.x * w), int(p1.y * h)
    x2, y2 = int(p2.x * w), int(p2.y * h)
    return np.hypot(x2 - x1, y2 - y1)


def analisar_movimentos(results, w, h):
    movimentos = []

    pose = results.pose_landmarks
    if pose is None:
        return ["sem corpo detectado"]

    lm = pose.landmark

    ombro_esq = lm[mp_holistic.PoseLandmark.LEFT_SHOULDER]
    ombro_dir = lm[mp_holistic.PoseLandmark.RIGHT_SHOULDER]
    mao_esq = lm[mp_holistic.PoseLandmark.LEFT_WRIST]
    mao_dir = lm[mp_holistic.PoseLandmark.RIGHT_WRIST]

    if mao_esq.y < ombro_esq.y:
        movimentos.append("mão esquerda levantada")
    if mao_dir.y < ombro_dir.y:
        movimentos.append("mão direita levantada")

    dist_maos = calc_dist_xy(mao_esq, mao_dir, w, h)
    if dist_maos > 0.6 * w:
        movimentos.append("braços abertos")

    dist_mao_esq_ombro_dir = calc_dist_xy(mao_esq, ombro_dir, w, h)
    dist_mao_dir_ombro_esq = calc_dist_xy(mao_dir, ombro_esq, w, h)

    if dist_mao_esq_ombro_dir < 0.25 * w:
        movimentos.append("braço esquerdo cruzado")
    if dist_mao_dir_ombro_esq < 0.25 * w:
        movimentos.append("braço direito cruzado")

    if not movimentos:
        movimentos.append("posição neutra")

    return movimentos


def main():
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Erro ao abrir webcam.")
        return

    with mp_holistic.Holistic(
        model_complexity=1,
        smooth_landmarks=True,
        enable_segmentation=False,
        min_detection_confidence=0.6,
        min_tracking_confidence=0.6,
    ) as holistic:

        while True:
            ret, frame = cap.read()
            if not ret:
                print("Erro ao capturar frame.")
                break

            h, w, _ = frame.shape

            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = holistic.process(rgb)

            # Fundo totalmente preto
            black_frame = np.zeros((h, w, 3), dtype=np.uint8)

            # ===== Esqueleto COMPLETO do corpo (POSE) =====
            if results.pose_landmarks:
                mp_drawing.draw_landmarks(
                    black_frame,
                    results.pose_landmarks,
                    mp_holistic.POSE_CONNECTIONS,
                    # pontos discretos (pequenos) em vermelho forte
                    mp_drawing.DrawingSpec(
                        color=(0, 0, 255), thickness=2, circle_radius=2
                    ),
                    # conexões em azul forte
                    mp_drawing.DrawingSpec(
                        color=(255, 0, 0), thickness=3, circle_radius=0
                    ),
                )

            # ===== MÃOS com pontos e conexões (mapeamento completo) =====
            if results.left_hand_landmarks:
                mp_drawing.draw_landmarks(
                    black_frame,
                    results.left_hand_landmarks,
                    mp_holistic.HAND_CONNECTIONS,
                    # pontos em amarelo
                    mp_drawing.DrawingSpec(
                        color=(0, 255, 255), thickness=2, circle_radius=3
                    ),
                    # conexões em ciano
                    mp_drawing.DrawingSpec(
                        color=(255, 255, 0), thickness=2, circle_radius=0
                    ),
                )

            if results.right_hand_landmarks:
                mp_drawing.draw_landmarks(
                    black_frame,
                    results.right_hand_landmarks,
                    mp_holistic.HAND_CONNECTIONS,
                    mp_drawing.DrawingSpec(
                        color=(0, 255, 255), thickness=2, circle_radius=3
                    ),
                    mp_drawing.DrawingSpec(
                        color=(255, 255, 0), thickness=2, circle_radius=0
                    ),
                )

            # ===== ROSTO: só “máscara” (malha), sem pontos =====
            if results.face_landmarks:
                mp_drawing.draw_landmarks(
                    black_frame,
                    results.face_landmarks,
                    mp_face_mesh.FACEMESH_TESSELATION,
                    # não desenhar pontos do rosto
                    mp_drawing.DrawingSpec(
                        color=(0, 0, 0), thickness=0, circle_radius=0
                    ),
                    # apenas a malha com linha fina, cor forte porém limpa
                    mp_drawing.DrawingSpec(
                        color=(0, 0, 200), thickness=1, circle_radius=0
                    ),
                )

            # Análise de movimentos (texto)
            movimentos = analisar_movimentos(results, w, h)
            cv2.rectangle(black_frame, (0, 0), (420, 70), (10, 10, 10), -1)
            y0 = 25
            for mov in movimentos:
                cv2.putText(
                    black_frame,
                    mov,
                    (10, y0),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (0, 255, 255),
                    2,
                    cv2.LINE_AA,
                )
                y0 += 22

            cv2.imshow("Esqueleto Completo no Fundo Preto", black_frame)

            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

    cap.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()